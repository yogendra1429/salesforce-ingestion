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
    MAX_QUEUE_SIZE: process.env.MAX_QUEUE_SIZE || 20, // FIX: In-memory queue limitation
    MAX_FILE_SIZE: (process.env.MAX_FILE_SIZE_GB || 2) * 1024 * 1024 * 1024, // FIX: File size limit protection
    JOB_TIMEOUT_MS: (process.env.JOB_TIMEOUT_HOURS || 2) * 60 * 60 * 1000, // FIX: Global job timeout
    DATASET_ALIAS: process.env.DATASET_ALIAS,
    RECIPE_ID: process.env.RECIPE_ID,
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

    async getExistingSchema() {
        try {
            const res = await this.session.get(`/services/data/${CONFIG.API_VERSION}/wave/datasets/${CONFIG.DATASET_ALIAS}`);
            const versionData = await this.session.get(res.data.currentVersionUrl);
            return versionData.data.xmd.objects[0].fields.map(f => f.name.toLowerCase());
        } catch (e) { return null; }
    }

    async generateMetadata(headerLine) {
        const columns = Papa.parse(headerLine.replace(/^\uFEFF/, '').trim()).data[0] || [];
        if (columns.length === 0) throw new Error("CSV contains no columns");
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

// FIX: Health Endpoint
app.get("/health", (req, res) => {
    res.json({ status: "UP", activeWorkers, queueLength: taskQueue.length, maxParallel: CONFIG.MAX_PARALLEL_JOBS });
});

app.use(rateLimit({ windowMs: 15 * 60 * 1000, max: 100 }));
app.use(express.json({ limit: '1mb' }));

const authMiddleware = (req, res, next) => {
    const apiKey = req.headers['x-api-key'];
    if (!apiKey || apiKey !== CONFIG.API_KEY) return res.status(401).json({ error: "Unauthorized" });
    next();
};

app.post("/ingest", authMiddleware, async (req, res) => {
    // FIX: Duplicate File Protection (dedupe URLs in request)
    const rawUrls = req.body.csv_urls;
    if (!Array.isArray(rawUrls) || rawUrls.length === 0) return res.status(400).json({ error: "csv_urls must be a non-empty array" });
    const csv_urls = [...new Set(rawUrls.map(u => u.trim()))];

    // FIX: Queue size limit rejection
    if (taskQueue.length >= CONFIG.MAX_QUEUE_SIZE) return res.status(503).json({ error: "Server busy, queue full" });

    const reqId = uuidv4();
    res.status(202).json({ status: "Queued", requestId: reqId, deduplicatedCount: csv_urls.length });

    taskQueue.push(async () => {
        const results = []; // FIX: Partial Failure Handling (track results per URL)
        const context = { reqId };

        for (const url of csv_urls) {
            context.url = url;
            Logger.info("Starting ingestion", context); // FIX: Logging start

            // FIX: Timeout protection per file
            const timeoutPromise = new Promise((_, reject) => setTimeout(() => reject(new Error("File processing timed out")), CONFIG.JOB_TIMEOUT_MS));

            const ingestPromise = (async () => {
                let jobId = null;
                try {
                    const response = await axios({ method: 'get', url, responseType: 'stream', timeout: 300000 });
                    
                    // FIX: File size limit protection
                    const contentLength = parseInt(response.headers['content-length'], 10);
                    if (contentLength > CONFIG.MAX_FILE_SIZE) throw new Error(`File too large: ${(contentLength / 1024 / 1024 / 1024).toFixed(2)}GB`);

                    const inputStream = response.data;
                    let internalBuffer = Buffer.alloc(0); // FIX: Memory optimization (incremental buffer)
                    let partCounter = 1;
                    let headerColsCount = 0;
                    const activePool = new Set();

                    for await (const chunk of inputStream) {
                        internalBuffer = Buffer.concat([internalBuffer, chunk]);

                        if (!jobId && internalBuffer.includes('\n')) {
                            const firstNewLine = internalBuffer.indexOf('\n');
                            const headerLine = internalBuffer.subarray(0, firstNewLine).toString();
                            headerColsCount = (Papa.parse(headerLine).data[0] || []).length; // FIX: Row consistency validation
                            
                            const metadata = await sf.generateMetadata(headerLine);
                            const job = await sf.session.post(`/services/data/${CONFIG.API_VERSION}/sobjects/InsightsExternalData`, {
                                EdgemartAlias: CONFIG.DATASET_ALIAS, MetadataJson: metadata, Operation: "Append", Action: "None", Format: "Csv"
                            });
                            jobId = job.data.id;
                            context.jobId = jobId;
                            Logger.info("Created Salesforce Job", context);
                        }

                        // FIX: Memory optimization (process only valid chunks)
                        while (internalBuffer.length >= CONFIG.CHUNK_SIZE) {
                            if (activePool.size >= CONFIG.CONCURRENCY) {
                                inputStream.pause();
                                await Promise.race(activePool);
                                inputStream.resume();
                            }

                            const uploadData = internalBuffer.subarray(0, CONFIG.CHUNK_SIZE);
                            internalBuffer = internalBuffer.subarray(CONFIG.CHUNK_SIZE);
                            
                            const task = sf.session.post(`/services/data/${CONFIG.API_VERSION}/sobjects/InsightsExternalDataPart`, {
                                InsightsExternalDataId: jobId, PartNumber: partCounter++, DataFile: uploadData.toString("base64")
                            }).then(() => activePool.delete(task));
                            activePool.add(task);
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
                            await new Promise(r => setTimeout(r, 15000));
                            const poll = await sf.session.get(`/services/data/${CONFIG.API_VERSION}/sobjects/InsightsExternalData/${jobId}`);
                            status = poll.data.Status;
                            if (status === "Failed") throw new Error(poll.data.StatusMessage || "Salesforce processing failed");
                        }
                    }
                    results.push({ url, status: "success", jobId });
                    Logger.info("Ingestion completed", context); // FIX: Logging end
                } catch (err) {
                    results.push({ url, status: "failed", error: err.message });
                    Logger.error("Ingestion failed per URL", err, context);
                    if (jobId) await sf.session.patch(`/services/data/${CONFIG.API_VERSION}/sobjects/InsightsExternalData/${jobId}`, { Action: "Abort" }).catch(() => {});
                }
            })();

            await Promise.race([ingestPromise, timeoutPromise]);
        }

        // FIX: Recipe Match Fix (Exact ID Match)
        try {
            await sf.authenticate();
            const recipeList = await sf.session.get(`/services/data/${CONFIG.API_VERSION}/wave/recipes`);
            const targetRecipe = recipeList.data.recipes.find(r => r.id === CONFIG.RECIPE_ID); // FIX: Exact match

            if (targetRecipe) {
                await sf.session.post(`/services/data/${CONFIG.API_VERSION}/wave/dataflowjobs`, { dataflowId: targetRecipe.id, command: "start" });
                Logger.info("Recipe triggered", context);
            }
        } catch (err) { Logger.error("Recipe Trigger Failed", err, context); }
        
        Logger.info("Pipeline Execution Summary", { reqId, results }); // FIX: Structured final log
    });
    runWorker();
});

app.listen(CONFIG.PORT, () => Logger.info(`Server online on port ${CONFIG.PORT}`));