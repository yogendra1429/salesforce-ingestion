const express = require("express");
const axios = require("axios");
const axiosRetry = require("axios-retry").default || require("axios-retry");
const Papa = require("papaparse");
const { v4: uuidv4 } = require('uuid');
const rateLimit = require("express-rate-limit");
require('dotenv').config();

const CONFIG = {
    PORT: process.env.PORT || 3000,
    CHUNK_SIZE: 5 * 1024 * 1024,
    CONCURRENCY: 3,
    MAX_PARALLEL_JOBS: 2,
    MAX_QUEUE_SIZE: process.env.MAX_QUEUE_SIZE || 20,
    MAX_FILE_SIZE: (process.env.MAX_FILE_SIZE_GB || 2) * 1024 * 1024 * 1024,
    JOB_TIMEOUT_MS: (process.env.JOB_TIMEOUT_HOURS || 2) * 60 * 60 * 1000,
    DATASET_ALIAS: process.env.DATASET_ALIAS,
    RECIPE_ID: process.env.RECIPE_ID, // Use 15 or 18 char ID starting with 05i
    ORG_DOMAIN: process.env.ORG_DOMAIN,
    API_KEY: process.env.API_KEY,
    AUTH: {
        client_id: process.env.CLIENT_ID,
        client_secret: process.env.CLIENT_SECRET,
        grant_type: "client_credentials" 
    },
    API_VERSION: "v60.0"
};

class Logger {
    static info(msg, context = {}) { 
        console.log(JSON.stringify({ timestamp: new Date().toISOString(), level: 'INFO', reqId: context.reqId, jobId: context.jobId, url: context.url, msg }));
    }
    static error(msg, err, context = {}) {
        const errorDetail = err?.response?.data || err?.message || err;
        console.error(JSON.stringify({ timestamp: new Date().toISOString(), level: 'ERROR', reqId: context.reqId, jobId: context.jobId, url: context.url, msg, error: errorDetail }));
    }
}

class SalesforceClient {
    constructor() {
        this.session = axios.create({ baseURL: CONFIG.ORG_DOMAIN, timeout: 300000 });
        axiosRetry(this.session, {
            retries: 5,
            retryDelay: axiosRetry.exponentialDelay,
            retryCondition: (error) => axiosRetry.isNetworkOrIdempotentRequestError(error) || error.response?.status === 429 || error.response?.status >= 500
        });
        this.session.interceptors.response.use(
            response => response,
            async (error) => {
                if (error.config && error.response && error.response.status === 401) {
                    await this.authenticate();
                    error.config.headers['Authorization'] = this.session.defaults.headers.common['Authorization'];
                    return this.session.request(error.config);
                }
                return Promise.reject(error);
            }
        );
    }

    async authenticate() {
        const params = new URLSearchParams(CONFIG.AUTH);
        const res = await axios.post(`${CONFIG.ORG_DOMAIN}/services/oauth2/token`, params);
        this.session.defaults.headers.common = { Authorization: `Bearer ${res.data.access_token}`, "Content-Type": "application/json" };
    }

    async generateMetadata(headerLine) {
        const columns = Papa.parse(headerLine.replace(/^\uFEFF/, '').trim()).data[0] || [];
        const numericFields = ["operation_count", "rows_processed", "request_size", "response_size", "http_status_code", "num_fields", "event_count"];
        const fields = columns.map(col => {
            const name = col.trim().replace(/[^a-zA-Z0-9]/g, '_').replace(/^_+|_+$/g, '');
            const lower = name.toLowerCase();
            if (lower.includes("timestamp") || lower.includes("date")) return { name, label: col, type: "Date", format: "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", fullyQualifiedName: name };
            if (numericFields.includes(lower)) return { name, label: col, type: "Numeric", precision: 18, scale: 0, defaultValue: "0", fullyQualifiedName: name };
            return { name, label: col, type: "Text", precision: 255, fullyQualifiedName: name };
        });
        return Buffer.from(JSON.stringify({ fileFormat: { charsetName: "UTF-8", fieldsEnclosedBy: "\"", numberOfLinesToSkip: 1 }, objects: [{ connector: "CSV", fullyQualifiedName: CONFIG.DATASET_ALIAS, label: CONFIG.DATASET_ALIAS, name: CONFIG.DATASET_ALIAS, fields }] })).toString("base64");
    }
}

const sf = new SalesforceClient();
const app = express();
const taskQueue = [];
let activeWorkers = 0;

const runWorker = async () => {
    if (activeWorkers >= CONFIG.MAX_PARALLEL_JOBS || taskQueue.length === 0) return;
    activeWorkers++;
    const task = taskQueue.shift();
    try { await task(); } finally {
        activeWorkers--;
        setImmediate(runWorker);
    }
};

app.use(rateLimit({ windowMs: 15 * 60 * 1000, max: 100 }));
app.use(express.json({ limit: '1mb' }));

app.post("/ingest", async (req, res) => {
    const apiKey = req.headers['x-api-key'];
    if (CONFIG.API_KEY && apiKey !== CONFIG.API_KEY) return res.status(401).json({ error: "Unauthorized" });

    const rawUrls = req.body.csv_urls;
    if (!Array.isArray(rawUrls) || rawUrls.length === 0) return res.status(400).json({ error: "csv_urls must be a non-empty array" });
    const csv_urls = [...new Set(rawUrls.map(u => u.trim()))];

    if (taskQueue.length >= CONFIG.MAX_QUEUE_SIZE) return res.status(503).json({ error: "Server busy" });

    const reqId = uuidv4();
    res.status(202).json({ status: "Queued", requestId: reqId });

    taskQueue.push(async () => {
        const results = [];
        const context = { reqId };

        for (const url of csv_urls) {
            context.url = url;
            const ingestPromise = (async () => {
                let jobId = null;
                try {
                    const response = await axios({ method: 'get', url, responseType: 'stream' });
                    const inputStream = response.data;
                    let internalBuffer = Buffer.alloc(0);
                    let partCounter = 1;
                    const activePool = new Set();

                    for await (const chunk of inputStream) {
                        internalBuffer = Buffer.concat([internalBuffer, chunk]);
                        if (!jobId && internalBuffer.includes('\n')) {
                            const headerLine = internalBuffer.subarray(0, internalBuffer.indexOf('\n')).toString();
                            const metadata = await sf.generateMetadata(headerLine);
                            const job = await sf.session.post(`/services/data/${CONFIG.API_VERSION}/sobjects/InsightsExternalData`, {
                                EdgemartAlias: CONFIG.DATASET_ALIAS, MetadataJson: metadata, Operation: "Append", Action: "None", Format: "Csv"
                            });
                            jobId = job.data.id;
                            context.jobId = jobId;
                        }

                        while (internalBuffer.length >= CONFIG.CHUNK_SIZE) {
                            const uploadData = internalBuffer.subarray(0, CONFIG.CHUNK_SIZE);
                            internalBuffer = internalBuffer.subarray(CONFIG.CHUNK_SIZE);
                            const task = sf.session.post(`/services/data/${CONFIG.API_VERSION}/sobjects/InsightsExternalDataPart`, {
                                InsightsExternalDataId: jobId, PartNumber: partCounter++, DataFile: uploadData.toString("base64")
                            }).then(() => activePool.delete(task));
                            activePool.add(task);
                            if (activePool.size >= CONFIG.CONCURRENCY) await Promise.race(activePool);
                        }
                    }
                    if (internalBuffer.length > 0 && jobId) {
                        await sf.session.post(`/services/data/${CONFIG.API_VERSION}/sobjects/InsightsExternalDataPart`, {
                            InsightsExternalDataId: jobId, PartNumber: partCounter, DataFile: internalBuffer.toString("base64")
                        });
                    }
                    await Promise.all(activePool);
                    if (jobId) {
                        await sf.session.patch(`/services/data/${CONFIG.API_VERSION}/sobjects/InsightsExternalData/${jobId}`, { Action: "Process" });
                        let status = "InProgress";
                        while (["New", "InProgress", "Queued"].includes(status)) {
                            await new Promise(r => setTimeout(r, 20000));
                            const poll = await sf.session.get(`/services/data/${CONFIG.API_VERSION}/sobjects/InsightsExternalData/${jobId}`);
                            status = poll.data.Status;
                        }
                    }
                    results.push({ url, status: "success" });
                } catch (err) {
                    Logger.error("Ingestion failed", err, context);
                    results.push({ url, status: "failed", error: err.message });
                }
            })();
            await ingestPromise; // Critical: Wait for file to finish before next one
        }

        // --- BULLETPROOF RECIPE TRIGGER ---
        try {
            await sf.authenticate();
            const recipeList = await sf.session.get(`/services/data/${CONFIG.API_VERSION}/wave/recipes`);
            // Find the recipe by 15-char prefix to avoid ID length issues
            const targetRecipe = recipeList.data.recipes.find(r => 
                r.id.startsWith(CONFIG.RECIPE_ID.substring(0, 15))
            );

            if (targetRecipe) {
                // Use containerId (02K) if available, otherwise targetDataflowId, fallback to ID
                const triggerId = targetRecipe.containerId || targetRecipe.targetDataflowId || targetRecipe.id;
                
                Logger.info(`Found Recipe: ${targetRecipe.label}. Triggering ID: ${triggerId}`, context);
                
                await sf.session.post(`/services/data/${CONFIG.API_VERSION}/wave/dataflowjobs`, { 
                    dataflowId: triggerId, 
                    command: "start" 
                });
                Logger.info("Recipe triggered successfully", context);
            } else {
                Logger.error(`Recipe with ID ${CONFIG.RECIPE_ID} not found in org.`, null, context);
            }
        } catch (err) {
            // Check if recipe is already running
            if (err.response?.data?.[0]?.message?.includes("already running")) {
                Logger.info("Recipe is already running. Skipping trigger.", context);
            } else {
                Logger.error("Recipe Trigger Failed", err, context);
            }
        }
    });
    runWorker();
});

app.listen(CONFIG.PORT, () => Logger.info(`Server running on port ${CONFIG.PORT}`));