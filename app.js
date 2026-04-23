const express = require("express");
const axios = require("axios");
const axiosRetry = require("axios-retry").default || require("axios-retry");
const { v4: uuidv4 } = require('uuid');
require('dotenv').config();

// Fix for high-concurrency event listener warnings
require('events').EventEmitter.defaultMaxListeners = 50;

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
        console.log("[Auth] Session Authenticated.");
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
            console.log(`[Schema] Using existing dataset fields: ${CONFIG.DATASET_ALIAS}`);
        } catch (e) {
            console.log(`[Schema] Generating metadata from CSV header...`);
            const cleanHeader = headerLine.replace(/^\uFEFF/, '').trim();
            const rawColumns = cleanHeader.split(/,(?=(?:(?:[^"]*"){2})*[^"]*$)/);
            const columns = rawColumns.map(c => c.trim().replace(/^"|"$/g, '')).filter(c => c.length > 0);
            
            if (columns.length === 0) throw "CSV header parsing failed";
            console.log(`[Debug] Parsed Columns:`, columns);

            const numericFields = ["operation_count", "rows_processed", "request_size", "response_size", "http_status_code", "num_fields", "event_count", "ui_event_sequence_num"];
            
            fields = columns.map((col, idx) => {
                const safeName = col.replace(/[^a-zA-Z0-9]/g, '_').replace(/^_+|_+$/g, '') || `Field_${idx}`;
                const lower = safeName.toLowerCase();
                if (lower.includes("timestamp") || lower.includes("date")) {
                    return { name: safeName, label: col, type: "Date", format: "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", fullyQualifiedName: safeName };
                }
                if (numericFields.includes(lower)) {
                    return { name: safeName, label: col, type: "Numeric", precision: 18, scale: 0, defaultValue: "0", fullyQualifiedName: safeName };
                }
                return { name: safeName, label: col, type: "Text", precision: 255, fullyQualifiedName: safeName };
            });

            // Requirement 4: Ensure at least one Text field
            if (!fields.some(f => f.type === "Text") && fields.length > 0) {
                fields[0].type = "Text";
                delete fields[0].format;
                fields[0].precision = 255;
            }
        }

        if (fields.length === 0) throw "Metadata validation failed: 0 fields found.";

        // Requirement 4: JSON Format Fix (Removed invalid properties)
        return Buffer.from(JSON.stringify({
            fileFormat: { charsetName: "UTF-8", fieldsEnclosedBy: "\"", fieldsDelimitedBy: "," },
            objects: [{
                connector: "CSV",
                fullyQualifiedName: CONFIG.DATASET_ALIAS,
                label: CONFIG.DATASET_ALIAS,
                name: CONFIG.DATASET_ALIAS,
                fields: fields
            }]
        })).toString("base64");
    }

    async triggerRecipe() {
        console.log(`[Recipe] Locating Recipe: ${CONFIG.RECIPE_ID}`);
        const listRes = await this.session.get(`/services/data/${CONFIG.API_VERSION}/wave/recipes`);
        const recipe = listRes.data.recipes.find(r => r.id === CONFIG.RECIPE_ID || r.id.startsWith(CONFIG.RECIPE_ID.substring(0, 15)));

        if (!recipe || !recipe.targetDataflowId) {
            throw new Error(`Recipe ID or TargetDataflowId not found for ${CONFIG.RECIPE_ID}`);
        }

        console.log(`[Recipe] Triggering Dataflow ID: ${recipe.targetDataflowId}`);
        await this.session.post(`/services/data/${CONFIG.API_VERSION}/wave/dataflowjobs`, {
            dataflowId: recipe.targetDataflowId,
            command: "start"
        });
        console.log("[Recipe] Start command issued successfully.");
    }
}

const sf = new SalesforceClient();
const app = express();
app.use(express.json());

app.post("/ingest", async (req, res) => {
    const csv_urls = req.body.csv_urls || [];
    const reqId = uuidv4().substring(0, 8);
    let allFilesSucceeded = true;

    res.status(202).json({ status: "Processing", requestId: reqId });

    (async () => {
        try {
            await sf.authenticate();
            for (const url of csv_urls) {
                console.log(`[${reqId}] Processing URL: ${url}`);
                
                // Requirement 1: Detect Salesforce Internal URLs
                if (url.includes("file.force.com") || url.includes("servlet.shepherd")) {
                    throw new Error("Salesforce file URLs are not publicly accessible. Use public CSV URL.");
                }

                const response = await axios({ method: 'get', url, responseType: 'stream' });
                
                // Requirement 2: Content-Type Validation
                const contentType = response.headers['content-type'] || "";
                if (!contentType.includes("csv") && !contentType.includes("text")) {
                    const sample = await axios.get(url, { headers: { Range: "bytes=0-200" } });
                    console.log(`[Debug] Response Start: ${sample.data}`);
                    throw new Error("Invalid CSV file (HTML or unauthorized response received)");
                }

                let jobId = null;
                let partCounter = 1;
                let buffer = Buffer.alloc(0);

                for await (const chunk of response.data) {
                    buffer = Buffer.concat([buffer, chunk]);
                    
                    // Requirement 3: Safe Header Extraction
                    if (!jobId && buffer.includes('\n')) {
                        const first200 = buffer.toString('utf8', 0, 200);
                        console.log(`[Debug] First 200 chars: ${first200}`);
                        
                        const headerLine = buffer.subarray(0, buffer.indexOf('\n')).toString();
                        const metadata = await sf.getMetadata(headerLine);
                        
                        const job = await sf.session.post(`/services/data/${CONFIG.API_VERSION}/sobjects/InsightsExternalData`, {
                            EdgemartAlias: CONFIG.DATASET_ALIAS,
                            MetadataJson: metadata,
                            Operation: "Append", Action: "None", Format: "Csv"
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

            // Requirement 5 & 6: Trigger recipe only on success
            if (allFilesSucceeded) {
                await sf.triggerRecipe();
            }

        } catch (err) {
            allFilesSucceeded = false;
            console.error(`[Fatal Error ${reqId}]`, err.response?.data || err.message);
            console.log("Skipping recipe due to ingestion failure.");
        }
    })();
});

app.listen(CONFIG.PORT, '0.0.0.0', () => console.log(`Ingestion Engine Live on Port ${CONFIG.PORT}`));
