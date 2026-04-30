const express = require("express");
const axios = require("axios");
const axiosRetry = require("axios-retry").default || require("axios-retry");
const rateLimit = require("express-rate-limit");
const { v4: uuidv4 } = require('uuid');
require('dotenv').config();
const app = express();
app.set('trust proxy', 1);
const CONFIG = {
    PORT: process.env.PORT || 3000,
    CHUNK_SIZE: 5 * 1024 * 1024, // 5MB
    MAX_CONCURRENT_UPLOADS: 3,
    MAX_FILE_SIZE: 2 * 1024 * 1024 * 1024, // 2GB
    POLL_MAX_RETRIES: 40, // ~10 minutes
    API_KEY: process.env.API_KEY,
    DATASET_ALIAS: process.env.DATASET_ALIAS || "AppAnalytics_Master",
    RECIPE_ID: process.env.RECIPE_ID,
    ORG_DOMAIN: process.env.ORG_DOMAIN,
    API_VERSION: "v60.0",
    AUTH: {
        client_id: process.env.CLIENT_ID,
        client_secret: process.env.CLIENT_SECRET,
        grant_type: "client_credentials"
    }
};

// Security Middleware
const authenticateAPI = (req, res, next) => {
    const key = req.headers['x-api-key'];
    if (!CONFIG.API_KEY || key === CONFIG.API_KEY) return next();
    res.status(401).json({ error: "Unauthorized" });
};

const limiter = rateLimit({ windowMs: 15 * 60 * 1000, max: 50 });

class SalesforceClient {
    constructor() {
        this.session = axios.create({ 
            baseURL: CONFIG.ORG_DOMAIN, 
            timeout: 300000,
            maxContentLength: Infinity,
            maxBodyLength: Infinity 
        });
        
        this.session.interceptors.response.use(
            (res) => res,
            async (err) => {
                const originalRequest = err.config;
                if (err.response?.status === 401 && !originalRequest._retry) {
                    originalRequest._retry = true;
                    await this.authenticate();
                    originalRequest.headers['Authorization'] = this.session.defaults.headers.common['Authorization'];
                    return this.session(originalRequest);
                }
                return Promise.reject(err);
            }
        );

        axiosRetry(this.session, { 
            retries: 3, 
            retryDelay: axiosRetry.exponentialDelay,
            retryCondition: (e) => axiosRetry.isNetworkOrIdempotentRequestError(e) || e.response?.status === 429
        });
    }

    async authenticate() {
        const params = new URLSearchParams(CONFIG.AUTH);
        const res = await axios.post(`${CONFIG.ORG_DOMAIN}/services/oauth2/token`, params);
        this.session.defaults.headers.common = { 
            Authorization: `Bearer ${res.data.access_token}`, 
            "Content-Type": "application/json" 
        };
        console.log("[Auth] Token Refreshed.");
    }

    async pollJobStatus(jobId, reqId) {
        let attempts = 0;
        while (attempts < CONFIG.POLL_MAX_RETRIES) {
            await new Promise(r => setTimeout(r, 15000));
            const res = await this.session.get(`/services/data/${CONFIG.API_VERSION}/sobjects/InsightsExternalData/${jobId}`);
            const { Status, StatusMessage } = res.data;
            
            console.log(`[${reqId}][Job: ${jobId}] Current Status: ${Status}`);
            if (Status === "Completed") return true;
            if (Status === "Failed") throw new Error(`SF Job Failed: ${StatusMessage}`);
            attempts++;
        }
        throw new Error("Polling timeout: Salesforce took too long to process.");
    }

    async abortJob(jobId, reqId) {
        if (!jobId) return;
        try {
            await this.session.patch(`/services/data/${CONFIG.API_VERSION}/sobjects/InsightsExternalData/${jobId}`, { Action: "Abort" });
            console.log(`[${reqId}][Job: ${jobId}] Cleanup: Job Aborted in Salesforce.`);
        } catch (e) {
            console.error(`[${reqId}] Cleanup Failed: Could not abort job ${jobId}`);
        }
    }

    async getMetadata(headerLine) {
        const cleanHeader = headerLine.replace(/^\uFEFF/, '').trim();
        const columns = cleanHeader.split(/,(?=(?:(?:[^"]*"){2})*[^"]*$)/).map(c => c.trim().replace(/^"|"$/g, '')).filter(c => c.length > 0);
        const fields = columns.map((col, idx) => ({
            name: col.replace(/[^a-zA-Z0-9]/g, '_') || `Field_${idx}`,
            label: col, type: "Text", precision: 255, fullyQualifiedName: col.replace(/[^a-zA-Z0-9]/g, '_')
        }));

        return Buffer.from(JSON.stringify({
            fileFormat: { charsetName: "UTF-8", fieldsEnclosedBy: "\"", fieldsDelimitedBy: "," },
            objects: [{ connector: "CSV", fullyQualifiedName: CONFIG.DATASET_ALIAS, label: CONFIG.DATASET_ALIAS, name: CONFIG.DATASET_ALIAS, fields }]
        })).toString("base64");
    }

    async triggerRecipe(reqId) {
        console.log(`[${reqId}] Triggering CRM Analytics Recipe...`);
        try {
            await this.session.post(`/services/data/${CONFIG.API_VERSION}/wave/recipes/${CONFIG.RECIPE_ID}/tasks`, { action: "run" });
            console.log(`[${reqId}] Recipe started via Task API.`);
        } catch (err) {
            const listRes = await this.session.get(`/services/data/${CONFIG.API_VERSION}/wave/recipes`);
            const recipe = listRes.data.recipes.find(r => r.id.startsWith(CONFIG.RECIPE_ID.substring(0, 15)));
            if (!recipe?.targetDataflowId) throw new Error("Could not resolve targetDataflowId.");
            await this.session.post(`/services/data/${CONFIG.API_VERSION}/wave/dataflowjobs`, { dataflowId: recipe.targetDataflowId, command: "start" });
            console.log(`[${reqId}] Recipe started via Dataflow Fallback.`);
        }
    }
}

const sf = new SalesforceClient();
const app = express();
app.use(limiter);
app.use(express.json());

app.post("/ingest", authenticateAPI, async (req, res) => {
    let { csv_urls } = req.body;
    if (!csv_urls || !Array.isArray(csv_urls)) return res.status(400).json({ error: "csv_urls must be an array" });
    
    const dedupedUrls = [...new Set(csv_urls)];
    const reqId = uuidv4().substring(0, 8);
    
    res.status(202).json({ 
        message: "Pipeline initialized", 
        requestId: reqId,
        files_queued: dedupedUrls.length 
    });

    // Background Process
    (async () => {
        let globalSuccess = true;
        try {
            await sf.authenticate();
            for (const url of dedupedUrls) {
                let currentJobId = null;
                let dataStream = null;
                try {
                    console.log(`[${reqId}] Ingesting: ${url}`);
                    const response = await axios({ method: 'get', url, responseType: 'stream', timeout: 60000 });
                    dataStream = response.data;

                    let partCounter = 1;
                    let activePool = new Set(); 
                    let internalBuffer = Buffer.alloc(0);

                    for await (const chunk of dataStream) {
                        internalBuffer = Buffer.concat([internalBuffer, chunk]);

                        if (!currentJobId && internalBuffer.indexOf('\n') !== -1) {
                            const headerLine = internalBuffer.subarray(0, internalBuffer.indexOf('\n')).toString();
                            if (headerLine.includes("<html>")) throw new Error("Target URL returned HTML/Login page instead of CSV.");
                            
                            const metadata = await sf.getMetadata(headerLine);
                            const job = await sf.session.post(`/services/data/${CONFIG.API_VERSION}/sobjects/InsightsExternalData`, {
                                EdgemartAlias: CONFIG.DATASET_ALIAS, MetadataJson: metadata,
                                Operation: "Overwrite", Action: "None", Format: "Csv"
                            });
                            currentJobId = job.data.id;
                        }

                        while (internalBuffer.length >= CONFIG.CHUNK_SIZE) {
                            const chunkToUpload = internalBuffer.subarray(0, CONFIG.CHUNK_SIZE);
                            internalBuffer = Buffer.from(internalBuffer.subarray(CONFIG.CHUNK_SIZE));

                            if (activePool.size >= CONFIG.MAX_CONCURRENT_UPLOADS) {
                                dataStream.pause();
                                await Promise.race(activePool);
                                dataStream.resume();
                            }

                            const p = sf.session.post(`/services/data/${CONFIG.API_VERSION}/sobjects/InsightsExternalDataPart`, {
                                InsightsExternalDataId: currentJobId, PartNumber: partCounter++, DataFile: chunkToUpload.toString("base64")
                            }).finally(() => activePool.delete(p));
                            activePool.add(p);
                        }
                    }

                    if (internalBuffer.length > 0 && currentJobId) {
                        await sf.session.post(`/services/data/${CONFIG.API_VERSION}/sobjects/InsightsExternalDataPart`, {
                            InsightsExternalDataId: currentJobId, PartNumber: partCounter, DataFile: internalBuffer.toString("base64")
                        });
                    }

                    await Promise.all(activePool);
                    if (currentJobId) {
                        await sf.session.patch(`/services/data/${CONFIG.API_VERSION}/sobjects/InsightsExternalData/${currentJobId}`, { Action: "Process" });
                        await sf.pollJobStatus(currentJobId, reqId);
                    }
                } catch (e) {
                    console.error(`[${reqId}] Error on ${url}: ${e.message}`);
                    globalSuccess = false;
                    if (dataStream) dataStream.destroy();
                    if (currentJobId) await sf.abortJob(currentJobId, reqId);
                }
            }

            if (globalSuccess) {
                await sf.triggerRecipe(reqId);
                console.log(`[${reqId}] Pipeline Completed Successfully.`);
            } else {
                console.warn(`[${reqId}] Pipeline finished with errors. Recipe NOT triggered.`);
            }
        } catch (fatal) {
            console.error(`[${reqId}] FATAL CRASH: ${fatal.message}`);
        }
    })();
});

app.listen(CONFIG.PORT, () => console.log(`Inbound Engine Active on Port ${CONFIG.PORT}`));
