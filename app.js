const express = require("express");
const axios = require("axios");
const fs = require("fs");
const fsPromises = require("fs").promises;
const Papa = require("papaparse");
const path = require("path");
require('dotenv').config();
 
const CONFIG = {
    PORT: process.env.PORT || 3000,
    CHUNK_SIZE: 5 * 1024 * 1024,
    CONCURRENCY: 3,              
    DATASET_ALIAS: process.env.DATASET_ALIAS,
    RECIPE_ID: process.env.RECIPE_ID,
    ORG_DOMAIN: process.env.ORG_DOMAIN,
    API_KEY: process.env.API_KEY, // Add this in your .env file
    AUTH: {
        client_id: process.env.CLIENT_ID,
        client_secret: process.env.CLIENT_SECRET,
        grant_type: "client_credentials"
    },
    API_VERSION: "v60.0",
    MAX_RETRIES: 3
};
 
class Logger {
    static info(msg) { console.log(`[${new Date().toISOString()}] INFO: ${msg}`); }
    static warn(msg) { console.warn(`[${new Date().toISOString()}] WARN: ${msg}`); }
    static error(msg, err) {
        console.error(`[${new Date().toISOString()}] ERROR: ${msg}`);
        if (err?.response) console.error(JSON.stringify(err.response.data, null, 2));
    }
    static stats(part) {
        const mem = Math.round(process.memoryUsage().heapUsed / 1024 / 1024);
        console.log(`[Stats] Part ${part} Uploaded | RAM: ${mem}MB`);
    }
}
 
class SalesforceClient {
    constructor() {
        this.session = axios.create({ baseURL: CONFIG.ORG_DOMAIN, timeout: 300000 });
    }
 
    async authenticate() {
        try {
            const params = new URLSearchParams(CONFIG.AUTH);
            const res = await axios.post(`${CONFIG.ORG_DOMAIN}/services/oauth2/token`, params);
            this.session.defaults.headers.common = {
                Authorization: `Bearer ${res.data.access_token}`,
                "Content-Type": "application/json"
            };
            Logger.info("Salesforce Auth Successful.");
            return true;
        } catch (e) { throw new Error(`Auth Failed: ${e.message}`); }
    }
 
    async call(fn, retries = CONFIG.MAX_RETRIES) {
        try {
            return await fn();
        } catch (err) {
            if (err.response?.status === 401 && retries > 0) {
                await this.authenticate();
                return this.call(fn, retries - 1);
            }
            if (retries > 0) {
                await new Promise(r => setTimeout(r, 5000));
                return this.call(fn, retries - 1);
            }
            throw err;
        }
    }
 
    async getMetadata(filePath) {
        const buffer = Buffer.alloc(65536);
        const fd = await fsPromises.open(filePath, 'r');
        await fd.read(buffer, 0, 65536, 0);
        await fd.close();
 
        const firstLine = buffer.toString().split('\n')[0].replace(/^\uFEFF/, '').trim();
        const columns = Papa.parse(firstLine).data[0] || [];
       
        const numericFields = [
            "operation_count", "rows_processed", "request_size",
            "response_size", "http_status_code", "num_fields", "event_count"
        ];
 
        const fields = columns.map(col => {
            const name = col.trim().replace(/[^a-zA-Z0-9]/g, '_').replace(/^_+|_+$/g, '');
            const lower = name.toLowerCase();
           
            if (lower.includes("timestamp") || lower.includes("date")) {
                return { name, label: col, type: "Date", format: "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", fullyQualifiedName: name };
            }
           
            if (numericFields.includes(lower)) {
                return {
                    name, label: col, type: "Numeric", precision: 18, scale: 0, defaultValue: "0", fullyQualifiedName: name
                };
            }
           
            return { name, label: col, type: "Text", precision: 255, fullyQualifiedName: name };
        });
 
        return Buffer.from(JSON.stringify({
            fileFormat: { charsetName: "UTF-8" },
            objects: [{
                connector: "CSV",
                fullyQualifiedName: CONFIG.DATASET_ALIAS,
                label: CONFIG.DATASET_ALIAS,
                name: CONFIG.DATASET_ALIAS,
                fields
            }]
        })).toString("base64");
    }
}
 
const sf = new SalesforceClient();
const app = express();
let isBusy = false;
 
app.use(express.json());
 
// --- Middleware for API Key Check ---
const authenticateApiKey = (req, res, next) => {
    const userApiKey = req.headers['x-api-key'];
   
    if (!CONFIG.API_KEY) {
        Logger.warn("Server API_KEY is not set in .env. Security is disabled.");
        return next();
    }
 
    if (!userApiKey || userApiKey !== CONFIG.API_KEY) {
        Logger.warn(`Unauthorized access attempt from IP: ${req.ip}`);
        return res.status(401).json({ error: "Unauthorized: Invalid or missing API Key" });
    }
    next();
};
 
app.post("/ingest", authenticateApiKey, async (req, res) => {
    if (!req.body || !Array.isArray(req.body.csv_urls)) {
        return res.status(400).json({ error: "Invalid Request: csv_urls array missing" });
    }
 
    if (isBusy) return res.status(429).json({ error: "Pipeline Busy" });
   
    const { csv_urls } = req.body;
    res.status(202).json({ status: "Accepted", operation: "Append" });
    isBusy = true;
 
    (async () => {
        let tempFile = "";
        try {
            await sf.authenticate();
 
            for (const url of csv_urls) {
                tempFile = path.join(__dirname, `ingest_tmp_${Date.now()}.csv`);
               
                Logger.info(`Downloading: ${url}`);
                const dl = await axios({ url: url.trim(), method: "GET", responseType: "stream" });
                const writer = fs.createWriteStream(tempFile);
                dl.data.pipe(writer);
                await new Promise((resolve, reject) => { writer.on("finish", resolve); writer.on("error", reject); });
 
                const metadata = await sf.getMetadata(tempFile);
               
                Logger.info("Initializing Append Job...");
                const job = await sf.call(() => sf.session.post(`/services/data/${CONFIG.API_VERSION}/sobjects/InsightsExternalData`, {
                    EdgemartAlias: CONFIG.DATASET_ALIAS,
                    Operation: "Append",
                    Action: "None",
                    Format: "Csv",
                    MetadataJson: metadata
                }));
 
                const jobId = job.data.id;
                const fileStream = fs.createReadStream(tempFile, { highWaterMark: CONFIG.CHUNK_SIZE });
               
                let partCounter = 1;
                let activePool = [];
 
                for await (const chunk of fileStream) {
                    const currentPart = partCounter++;
                    const task = sf.call(() => sf.session.post(`/services/data/${CONFIG.API_VERSION}/sobjects/InsightsExternalDataPart`, {
                        InsightsExternalDataId: jobId, PartNumber: currentPart, DataFile: chunk.toString("base64")
                    })).then(() => Logger.stats(currentPart));
 
                    activePool.push(task);
                    if (activePool.length >= CONFIG.CONCURRENCY) {
                        await Promise.race(activePool);
                        activePool = activePool.filter(p => p.status === 'pending');
                    }
                }
                await Promise.all(activePool);
 
                Logger.info("Finalizing Salesforce Processing...");
                await sf.session.patch(`/services/data/${CONFIG.API_VERSION}/sobjects/InsightsExternalData/${jobId}`, { Action: "Process" });
 
                let status = "InProgress";
                while (["New", "InProgress", "Queued"].includes(status)) {
                    await new Promise(r => setTimeout(r, 30000));
                    const poll = await sf.session.get(`/services/data/${CONFIG.API_VERSION}/sobjects/InsightsExternalData/${jobId}`);
                    status = poll.data.Status;
                    Logger.info(`Job Status: ${status}`);
                    if (status === "Failed") throw new Error(poll.data.StatusMessage);
                }
               
                await fsPromises.unlink(tempFile).catch(() => {});
                tempFile = "";
            }
 
            Logger.info("All Append Jobs Successful. Triggering Recipe...");
            const recipeList = await sf.call(() => sf.session.get(`/services/data/${CONFIG.API_VERSION}/wave/recipes`));
            const targetRecipe = recipeList.data.recipes.find(r => r.id.startsWith(CONFIG.RECIPE_ID.substring(0, 15)));
           
            await sf.session.post(`/services/data/${CONFIG.API_VERSION}/wave/dataflowjobs`, {
                dataflowId: targetRecipe?.targetDataflowId || CONFIG.RECIPE_ID, command: "start"
            });
            Logger.info("PIPELINE COMPLETE");
 
        } catch (err) {
            Logger.error("Pipeline Crashed", err);
            if (tempFile) await fsPromises.unlink(tempFile).catch(() => {});
        } finally {
            isBusy = false;
        }
    })();
});
 
app.listen(CONFIG.PORT, () => Logger.info(`Service running on port ${CONFIG.PORT}`));