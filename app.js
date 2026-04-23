const express = require("express");
const axios = require("axios");
const axiosRetry = require("axios-retry").default || require("axios-retry");
const { v4: uuidv4 } = require('uuid');
require('dotenv').config();

// Fix for EventEmitter warning
require('events').EventEmitter.defaultMaxListeners = 30;

const CONFIG = {
    PORT: process.env.PORT || 3000,
    CHUNK_SIZE: 5 * 1024 * 1024,
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

class SalesforceClient {
    constructor() {
        this.session = axios.create({ baseURL: CONFIG.ORG_DOMAIN, timeout: 600000 });
        axiosRetry(this.session, { 
            retries: 5, 
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
        console.log("[Auth] Salesforce Session Authenticated.");
    }

    async getMetadata(headerLine) {
        let fields = [];
        try {
            const ds = await this.session.get(`/services/data/${CONFIG.API_VERSION}/wave/datasets/${CONFIG.DATASET_ALIAS}`);
            const version = await this.session.get(ds.data.currentVersionUrl);
            fields = version.data.xmd.objects[0].fields.map(f => ({
                name: f.name, label: f.label || f.name, type: f.type,
                format: f.format, precision: f.precision, scale: f.scale, fullyQualifiedName: f.name
            }));
            console.log(`[Schema] Syncing with existing dataset: ${CONFIG.DATASET_ALIAS}`);
        } catch (e) {
            console.log(`[Schema] Generating new metadata from CSV header.`);
            const columns = headerLine.replace(/^\uFEFF/, '').split(',').map(c => c.trim().replace(/"/g, ''));
            const numericFields = ["operation_count", "rows_processed", "request_size", "response_size", "http_status_code", "num_fields", "event_count", "ui_event_sequence_num"];
            
            fields = columns.map(col => {
                const name = col.replace(/[^a-zA-Z0-9]/g, '_');
                const lower = name.toLowerCase();
                if (lower.includes("timestamp") || lower.includes("date")) {
                    return { name, label: col, type: "Date", format: "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", fullyQualifiedName: name };
                }
                if (numericFields.includes(lower)) {
                    return { name, label: col, type: "Numeric", precision: 18, scale: 0, defaultValue: "0", fullyQualifiedName: name };
                }
                return { name, label: col, type: "Text", precision: 255, fullyQualifiedName: name };
            });
        }
        return Buffer.from(JSON.stringify({
            fileFormat: { charsetName: "UTF-8", fieldsEnclosedBy: "\"", numberOfLinesToSkip: 1 },
            objects: [{ connector: "CSV", fullyQualifiedName: CONFIG.DATASET_ALIAS, label: CONFIG.DATASET_ALIAS, name: CONFIG.DATASET_ALIAS, fields }]
        })).toString("base64");
    }

    // Integrated your working logic
    async runRecipe() {
        console.log(`[Recipe] Initiating Scan: ${CONFIG.RECIPE_ID}`);
        let triggerId = CONFIG.RECIPE_ID;
        const headers = this.session.defaults.headers.common;

        try {
            const listRes = await this.session.get(`/services/data/${CONFIG.API_VERSION}/wave/recipes`);
            const allRecipes = listRes.data.recipes || [];
            const found = allRecipes.find(r => r.id === CONFIG.RECIPE_ID || r.id.substring(0, 15) === CONFIG.RECIPE_ID.substring(0, 15));

            if (found && found.targetDataflowId) {
                triggerId = found.targetDataflowId;
                console.log(`[Recipe] Resolved Target ID: ${triggerId}`);
            }
        } catch (e) {
            console.warn("[Recipe] Discovery scan bypassed. Using default ID.");
        }

        try {
            await this.session.post(`/services/data/${CONFIG.API_VERSION}/wave/dataflowjobs`, { 
                dataflowId: triggerId, 
                command: "start" 
            });
            console.log("[Recipe] Trigger success (Dataflow API)");
        } catch (err) {
            console.warn("[Recipe] Primary trigger failed. Attempting Task API fallback...");
            try {
                await this.session.post(`/services/data/${CONFIG.API_VERSION}/wave/recipes/${CONFIG.RECIPE_ID}/tasks`, { 
                    action: "run" 
                });
                console.log("[Recipe] Trigger success (Task API)");
            } catch (fallbackErr) {
                console.error("[Recipe] Critical: All trigger methods failed.");
            }
        }
    }
}

const sf = new SalesforceClient();
const app = express();
app.use(express.json());

app.post("/ingest", async (req, res) => {
    const csv_urls = req.body.csv_urls || [];
    const reqId = uuidv4().substring(0, 8);
    res.status(202).json({ status: "Processing", requestId: reqId });

    (async () => {
        try {
            await sf.authenticate();
            for (const url of csv_urls) {
                console.log(`[${reqId}] Ingesting: ${url}`);
                let jobId = null;
                let partCounter = 1;
                let buffer = Buffer.alloc(0);

                const response = await axios({ method: 'get', url, responseType: 'stream' });

                for await (const chunk of response.data) {
                    buffer = Buffer.concat([buffer, chunk]);
                    if (!jobId && buffer.includes('\n')) {
                        const headerLine = buffer.subarray(0, buffer.indexOf('\n')).toString();
                        const metadata = await sf.getMetadata(headerLine);
                        const job = await sf.session.post(`/services/data/${CONFIG.API_VERSION}/sobjects/InsightsExternalData`, {
                            EdgemartAlias: CONFIG.DATASET_ALIAS,
                            MetadataJson: metadata,
                            Operation: "Overwrite", Action: "None", Format: "Csv"
                        });
                        jobId = job.data.id;
                    }
                    while (buffer.length >= CONFIG.CHUNK_SIZE) {
                        const chunkData = buffer.subarray(0, CONFIG.CHUNK_SIZE);
                        buffer = buffer.subarray(CONFIG.CHUNK_SIZE);
                        await sf.session.post(`/services/data/${CONFIG.API_VERSION}/sobjects/InsightsExternalDataPart`, {
                            InsightsExternalDataId: jobId, PartNumber: partCounter++, DataFile: chunkData.toString("base64")
                        });
                    }
                }
                if (buffer.length > 0 && jobId) {
                    await sf.session.post(`/services/data/${CONFIG.API_VERSION}/sobjects/InsightsExternalDataPart`, {
                        InsightsExternalDataId: jobId, PartNumber: partCounter, DataFile: buffer.toString("base64")
                    });
                }
                if (jobId) await sf.session.patch(`/services/data/${CONFIG.API_VERSION}/sobjects/InsightsExternalData/${jobId}`, { Action: "Process" });
            }

            // Run the updated Recipe logic
            await sf.runRecipe();

        } catch (err) {
            console.error(`[Fatal Error ${reqId}]`, err.response?.data || err.message);
        }
    })();
});

app.listen(CONFIG.PORT, '0.0.0.0', () => {
    console.log(`=================================`);
    console.log(`Salesforce Ingestion Engine`);
    console.log(`Port: ${CONFIG.PORT}`);
    console.log(`=================================`);
});
