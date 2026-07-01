import html from '../style/index.html';

// --- VAULT SECURITY HELPERS (AES-GCM) ---
async function getMasterKey(secret) {
    const enc = new TextEncoder();
    const keyMaterial = await crypto.subtle.importKey("raw", enc.encode(secret), "PBKDF2", false, ["deriveKey"]);
    return crypto.subtle.deriveKey(
        { name: "PBKDF2", salt: enc.encode("outfeed-salt"), iterations: 100000, hash: "SHA-256" },
        keyMaterial,
        { name: "AES-GCM", length: 256 },
        false,
        ["encrypt", "decrypt"]
    );
}

async function encryptSecret(text, masterKeySecret) {
    const key = await getMasterKey(masterKeySecret);
    const iv = crypto.getRandomValues(new Uint8Array(12));
    const encoded = new TextEncoder().encode(text);
    const encrypted = await crypto.subtle.encrypt({ name: "AES-GCM", iv }, key, encoded);
    const combined = new Uint8Array(iv.length + encrypted.byteLength);
    combined.set(iv);
    combined.set(new Uint8Array(encrypted), iv.length);
    return Array.from(combined).map(b => b.toString(16).padStart(2, '0')).join('');
}

async function decryptSecret(hex, masterKeySecret) {
    if (!hex) return null;
    const key = await getMasterKey(masterKeySecret);
    const combined = new Uint8Array(hex.match(/.{1,2}/g).map(byte => parseInt(byte, 16)));
    const iv = combined.slice(0, 12);
    const data = combined.slice(12);
    const decrypted = await crypto.subtle.decrypt({ name: "AES-GCM", iv }, key, data);
    return new TextDecoder().decode(decrypted);
}

// --- ETL LOGIC ---
async function runExtraction(env, boardId) {
    console.log(`[ETL] Starting Extraction for Board: ${boardId}`);
    
    // 1. Fetch Board Config & Credentials from Vault
    const board = await env.mls_data.prepare('SELECT * FROM boards WHERE id = ?').bind(boardId).first();
    if (!board) throw new Error(`Board ${boardId} not found in registry`);

    let accessToken = board.last_token;
    
    // 2. TOKEN MANAGEMENT: Check if we need a fresh OAuth token
    const now = new Date();
    const expires = board.token_expires_at ? new Date(board.token_expires_at) : new Date(0);

    if (board.auth_type === 'oauth2' && board.client_id && (!accessToken || now >= expires)) {
        console.log(`[ETL] Fetching fresh OAuth token for ${boardId}...`);
        const clientSecret = await decryptSecret(board.encrypted_client_secret, env.BOARD_ENCRYPTION_KEY);
        
        const response = await fetch(board.auth_endpoint, {
            method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body: new URLSearchParams({
                grant_type: 'client_credentials',
                client_id: board.client_id,
                client_secret: clientSecret,
                scope: 'api'
            })
        });

        if (!response.ok) throw new Error(`OAuth failed: ${await response.text()}`);
        
        const tokenData = await response.json();
        accessToken = tokenData.access_token;
        const newExpires = new Date(Date.now() + (tokenData.expires_in * 1000) - 60000); // 1 min buffer

        // Cache the token
        await env.mls_data.prepare('UPDATE boards SET last_token = ?, token_expires_at = ? WHERE id = ?')
            .bind(accessToken, newExpires.toISOString(), boardId).run();
    }

    // 3. MOCK DATA (Fallback to Mock for now)
    const mockListings = [
        { id: `${boardId.toUpperCase()}-${Math.floor(Math.random() * 100000)}`, status: 'Active', price: 850000, address: '123 Spadina Ave', city: 'Toronto', bedrooms: 3, bathrooms: 2, sqft: 1500, agent_id: 'A123', office_id: 'O999', image_url: 'https://images.unsplash.com/photo-1570129477492-45c003edd2be?auto=format&fit=crop&w=800&q=80' },
        { id: `${boardId.toUpperCase()}-${Math.floor(Math.random() * 100000)}`, status: 'Pending', price: 620000, address: '456 King St W', city: 'Toronto', bedrooms: 2, bathrooms: 1, sqft: 900, agent_id: 'A456', office_id: 'O888', image_url: 'https://images.unsplash.com/photo-1568605114967-8130f3a36994?auto=format&fit=crop&w=800&q=80' },
        { id: `${boardId.toUpperCase()}-${Math.floor(Math.random() * 100000)}`, status: 'Active', price: 1200000, address: '789 Queen St E', city: 'Toronto', bedrooms: 4, bathrooms: 3, sqft: 2200, agent_id: 'A123', office_id: 'O999', image_url: 'https://images.unsplash.com/photo-1512917774080-9991f1c4c750?auto=format&fit=crop&w=800&q=80' },
        { id: `CRMLS-${Math.floor(Math.random() * 100000)}`, status: 'Active', price: 450000, address: '123 Oil St', city: 'Bakersfield', bedrooms: 3, bathrooms: 2, sqft: 1800, agent_id: 'B999', office_id: 'O111', image_url: 'https://images.unsplash.com/photo-1580587767526-cf3671a050d4?auto=format&fit=crop&w=800&q=80' }
    ];
    
    // 2. PIPE to Data Lake (Cloudflare Pipelines)
    if (env.MLS_PIPELINE && typeof env.MLS_PIPELINE.send === 'function') {
        try {
            await env.MLS_PIPELINE.send(mockListings.map(l => ({
                ...l,
                board_id: boardId,
                timestamp: new Date().toISOString()
            })));
        } catch (e) {
            console.error('Pipeline Error:', e);
        }
    }

    // 3. STORE RAW DATA IN R2 (Master Record)
    if (env.RAW_DATA_LAKE) {
        try {
            const rawPayload = JSON.stringify({ board_id: boardId, timestamp: new Date().toISOString(), listings: mockListings });
            await env.RAW_DATA_LAKE.put(`raw/${boardId}/${Date.now()}.json`, rawPayload);
        } catch (e) {
            console.error('R2 Storage Error:', e);
        }
    }

    // 4. LOAD into Central Control Plane D1 (Lean Cache)
    const stmt = env.mls_data.prepare(`
        INSERT INTO listings (id, board_id, status, price, address, city, bedrooms, bathrooms, sqft, agent_id, office_id, image_url) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET 
            status=excluded.status, price=excluded.price, updated_at=CURRENT_TIMESTAMP
    `);

    const batch = mockListings.map(l => stmt.bind(
        l.id, boardId, l.status, l.price, l.address, l.city, l.bedrooms, l.bathrooms, l.sqft, l.agent_id, l.office_id, l.image_url
    ));
    
    await env.mls_data.batch(batch);

    return {
        records: mockListings.length,
        source: `${boardId.toUpperCase()} RESO API`,
        destination: `Central Cache (D1) + Data Lake (R2/Pipelines)`,
        status: 'success'
    };
}

export default {
    async fetch(request, env, ctx) {
        const url = new URL(request.url);
        const path = url.pathname;
        const method = request.method;
        const parts = path.split('/').filter(Boolean);
        const [reqBoardId, section, action] = parts;

        // --- PUBLIC API ENDPOINT ---
        if (path === '/api/listings' && method === 'GET') {
            const apiKey = request.headers.get('Authorization');
            if (!apiKey) return Response.json({ success: false, error: 'Unauthorized' }, { status: 401 });

            // 1. LOOKUP CLIENT & THEIR MARKET FILTER
            const client = await env.mls_data.prepare('SELECT id, name, market_filter, enabled_fields FROM clients WHERE api_key = ? AND status = "active"')
                .bind(apiKey).first();
            
            if (!client) return Response.json({ success: false, error: 'Invalid API Key' }, { status: 401 });

            // 2. PARSE MARKET FILTER
            const market = client.market_filter ? JSON.parse(client.market_filter) : {};
            // 3. BUILD DYNAMIC SQL (Enforcing Market Guard)
            let sql = 'SELECT * FROM listings WHERE 1=1';
            const params = [];

            // ALWAYS ENFORCE CLIENT'S MARKET FILTER
            if (market.city) {
                sql += ' AND city = ?';
                params.push(market.city);
            }
            if (market.agent_id) {
                sql += ' AND agent_id = ?';
                params.push(market.agent_id);
            }
            if (market.office_id) {
                sql += ' AND office_id = ?';
                params.push(market.office_id);
            }

            // OPTIONAL: Let client filter further (e.g. by status)
            const filterStatus = url.searchParams.get('status');
            if (filterStatus) {
                sql += ' AND status = ?';
                params.push(filterStatus);
            }

            try {
                const { results } = await env.mls_data.prepare(sql).bind(...params).all();
                
                // 4. APPLY FIELD OPT-INS (Standardized Schema)
                const enabled = client.enabled_fields ? JSON.parse(client.enabled_fields) : [];
                
                const filteredResults = results.map(row => {
                    if (enabled.length === 0) return row; // Default to all if none specified
                    
                    const filtered = {};
                    enabled.forEach(field => {
                        if (row.hasOwnProperty(field)) {
                            filtered[field] = row[field];
                        }
                    });
                    return filtered;
                });

                return Response.json({ 
                    success: true, 
                    client: client.name,
                    market: market,
                    fields: enabled.length > 0 ? enabled : 'all',
                    count: filteredResults.length, 
                    data: filteredResults 
                }, {
                    headers: { 'Access-Control-Allow-Origin': '*' }
                });
            } catch (e) {
                return Response.json({ success: false, error: e.message }, { status: 500 });
            }
        }

        // --- VAULT API: SAVE CREDENTIALS ---
        if (url.pathname === '/api/vault/save' && request.method === 'POST') {
            try {
                const { boardId, authType, authEndpoint, clientId, clientSecret } = await request.json();
                if (!boardId || !clientSecret) return Response.json({ success: false, error: 'Missing data' }, { status: 400 });

                // Encrypt the secret using the master key
                const encrypted = await encryptSecret(clientSecret, env.BOARD_ENCRYPTION_KEY);

                await env.mls_data.prepare(`
                    UPDATE boards SET 
                        auth_type = ?, 
                        auth_endpoint = ?, 
                        client_id = ?, 
                        encrypted_client_secret = ? 
                    WHERE id = ?
                `).bind(authType, authEndpoint, clientId, encrypted, boardId).run();

                return Response.json({ success: true, message: 'Credentials securely vaulted' });
            } catch (e) {
                return Response.json({ success: false, error: e.message }, { status: 500 });
            }
        }

        if (action === 'run' && method === 'POST') {
            try {
                const extractionResults = await runExtraction(reqBoardId, env);
                return new Response(JSON.stringify({ success: true, results: extractionResults }), { headers: { 'Content-Type': 'application/json' } });
            } catch (e) {
                return new Response(JSON.stringify({ success: false, error: e.message }), { status: 500, headers: { 'Content-Type': 'application/json' } });
            }
        }

        if (method === 'GET' && section === 'load' && action === 'data') {
            try {
                const { results } = await env.mls_data.prepare('SELECT * FROM listings WHERE board_id = ? ORDER BY updated_at DESC LIMIT 50').bind(reqBoardId).all();
                return new Response(JSON.stringify({ success: true, data: results }), { headers: { 'Content-Type': 'application/json' } });
            } catch (e) {
                return new Response(JSON.stringify({ success: false, error: e.message }), { status: 500, headers: { 'Content-Type': 'application/json' } });
            }
        }

        // Serve the UI Console HTML
        if (method === 'GET') {
            if (parts.length <= 2 && !path.endsWith('.json')) {
                try {
                    const { results: boards } = await env.mls_data.prepare("SELECT * FROM boards").all();
                    
                    if (boards.length === 0) {
                        return new Response("No boards found.", { status: 404 });
                    }
                    
                    const targetEntity = boards.find(b => b.id === reqBoardId) || boards[0];
                    const targetId = targetEntity.id;
                    const activeSection = ['overview', 'extract', 'transform', 'load'].includes(section) ? section : 'overview';
                    
                    const res = new Response(html, { headers: { 'Content-Type': 'text/html; charset=utf-8' } });
                    
                    return new HTMLRewriter()
                        .on('head', {
                            element(el) {
                                el.append(`<script>window.__DATA__ = ${JSON.stringify({ boards, clients: [] })};</script>`, { html: true });
                            }
                        })
                        .on('nav', {
                            element(el) {
                                const links = boards.map(b => `<a href="/${b.id}/${activeSection}" data-id="${b.id}" ${b.id === targetId ? 'aria-current="page"' : ''}>${b.name}</a>`).join('');
                                
                                const toggleHref = 'http://localhost:8788';
                                const toggleIcon = `<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"></path><circle cx="9" cy="7" r="4"></circle><path d="M23 21v-2a4 4 0 0 0-3-3.87"></path><path d="M16 3.13a4 4 0 0 1 0 7.75"></path></svg>`;
                                
                                const html = `
                                    <div style="display: flex; justify-content: flex-start; align-items: center; gap: 0.75rem; padding: 0.6rem 0.75rem; border: 1px solid transparent; color: var(--text-main); font-size: 0.875rem; font-weight: 500;">
                                        <span style="display: flex; align-items: center; color: var(--text-muted);">
                                            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                                                <path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z"></path>
                                                <polyline points="3.27 6.96 12 12.01 20.73 6.96"></polyline>
                                                <line x1="12" y1="22.08" x2="12" y2="12"></line>
                                            </svg>
                                        </span>
                                        Boards
                                    </div>
                                    <div style="flex: 1; display: flex; flex-direction: column; overflow-y: auto;">
                                        ${links}
                                    </div>
                                `;
                                el.setInnerContent(html, { html: true });
                            }
                        })
                        .on('aside', {
                            element(el) {
                                el.setInnerContent(`
                                    <a href="/${targetId}/overview" data-id="overview" ${activeSection === 'overview' ? 'aria-current="page"' : ''}>Overview</a>
                                    <a href="/${targetId}/extract" data-id="extract" ${activeSection === 'extract' ? 'aria-current="page"' : ''}>Extract</a>
                                    <a href="/${targetId}/transform" data-id="transform" ${activeSection === 'transform' ? 'aria-current="page"' : ''}>Transform</a>
                                    <a href="/${targetId}/load" data-id="load" ${activeSection === 'load' ? 'aria-current="page"' : ''}>Load</a>
                                `, { html: true });
                            }
                        })
                        .on('main > section', {
                            element(el) {
                                const id = el.getAttribute('id');
                                if (id === `view-${activeSection}`) {
                                    el.removeAttribute('hidden');
                                } else {
                                    el.setAttribute('hidden', '');
                                }
                            }
                        })
                        .on(`h2#ui-${activeSection}-title`, {
                            element(el) {
                                const map = { overview: 'Overview', extract: 'Extract', transform: 'Transform', load: 'Load' };
                                el.setInnerContent(`${map[activeSection] || 'Overview'}`);
                            }
                        })
                        .on('div#overview-content', {
                            element(el) {
                                if (activeSection === 'overview') {
                                    el.setInnerContent(`
                                        <article data-full>
                                            <table>
                                                <tbody>
                                                    <tr><th style="width: 200px;">Board Name</th><td>${targetEntity.name}</td></tr>
                                                    <tr><th>Short Name</th><td>${targetEntity.short_name}</td></tr>
                                                    <tr><th>RESO API URL</th><td><input type="text" id="vault-reso-url" value="${targetEntity.reso_url}" class="vault-input" style="border:none; background:transparent; width:100%; font-family:var(--font-mono); font-size:inherit; color:inherit; outline:none;"></td></tr>
                                                    <tr><th>Auth Endpoint</th><td><input type="text" id="vault-auth-endpoint" value="${targetEntity.auth_endpoint || ''}" class="vault-input" placeholder="https://..." style="border:none; background:transparent; width:100%; font-family:var(--font-mono); font-size:inherit; color:inherit; outline:none;"></td></tr>
                                                    <tr><th>Client ID</th><td><input type="text" id="vault-client-id" value="${targetEntity.client_id || ''}" class="vault-input" placeholder="ID..." style="border:none; background:transparent; width:100%; font-family:var(--font-mono); font-size:inherit; color:inherit; outline:none;"></td></tr>
                                                    <tr><th>Client Secret</th><td><input type="password" id="vault-client-secret" class="vault-input" placeholder="••••••••" style="border:none; background:transparent; width:100%; font-family:var(--font-mono); font-size:inherit; color:inherit; outline:none;"></td></tr>
                                                </tbody>
                                            </table>
                                            <div style="margin-top: 1rem; text-align: right; font-size: 0.75rem; color: var(--text-muted); min-height: 1rem;">
                                                <span id="vault-status"></span>
                                            </div>
                                        </article>
                                    `, { html: true });
                                }
                            }
                        })
                        .on('article#ui-extract-content', {
                            element(el) {
                                if (activeSection === 'extract') {
                                    el.setInnerContent(`
                                        <div class="form-actions" style="padding-left: 0;">
                                            <div class="form-actions-group">
                                                <button id="btn-extract">Run Full Extraction Job</button>
                                            </div>
                                        </div>
                                    `, { html: true });
                                }
                            }
                        })
                        .on('section#view-transform article', {
                            element(el) {
                                if (activeSection === 'transform') {
                                    el.setInnerContent(`
                                        <div style="margin-bottom: 1.5rem;">
                                            <p style="font-size: 0.875rem; color: var(--text-muted); margin-bottom: 1.5rem;">
                                                Configure data <strong>Transformations</strong> for your customers. Each API key can be tailored to specific locations and multiple MLS boards.
                                            </p>
                                            
                                            <table style="width: 100%; border: 1px solid var(--border); border-radius: var(--radius); overflow: hidden;">
                                                <thead>
                                                    <tr>
                                                        <th>Client / Key</th>
                                                        <th>Market Scope</th>
                                                        <th>Board Access</th>
                                                        <th>Storage</th>
                                                    </tr>
                                                </thead>
                                                <tbody>
                                                    <tr>
                                                        <td>
                                                            <strong>Curaytor</strong><br>
                                                            <code style="font-size: 0.7rem; color: var(--text-muted);">key_curaytor_123</code>
                                                        </td>
                                                        <td><span style="background: var(--bg-selected); padding: 2px 6px; border-radius: 4px; font-size: 0.75rem;">Toronto, ON</span></td>
                                                        <td>TRREB</td>
                                                        <td><span style="color: green;">● Shared D1</span></td>
                                                    </tr>
                                                    <tr>
                                                        <td>
                                                            <strong>Bakersfield Realty</strong><br>
                                                            <code style="font-size: 0.7rem; color: var(--text-muted);">key_bakersfield_789</code>
                                                        </td>
                                                        <td><span style="background: var(--bg-selected); padding: 2px 6px; border-radius: 4px; font-size: 0.75rem;">Bakersfield, CA</span></td>
                                                        <td>CRMLS</td>
                                                        <td><span style="color: var(--accent);">● Dedicated D1</span></td>
                                                    </tr>
                                                    <tr>
                                                        <td>
                                                            <strong>Global Enterprise</strong><br>
                                                            <code style="font-size: 0.7rem; color: var(--text-muted);">key_global_all</code>
                                                        </td>
                                                        <td><span style="background: var(--bg-selected); padding: 2px 6px; border-radius: 4px; font-size: 0.75rem;">All Markets</span></td>
                                                        <td>All Boards</td>
                                                        <td><span style="color: var(--accent);">● Dedicated D1</span></td>
                                                    </tr>
                                                </tbody>
                                            </table>
                                        </div>

                                        <div class="form-actions" style="padding-left: 0;">
                                            <button id="btn-create-feed">Issue New Client Feed Key</button>
                                        </div>
                                    `, { html: true });
                                }
                            }
                        })
                        .on('article#ui-load-content', {
                            element(el) {
                                if (activeSection === 'load') {
                                    el.setInnerContent(`
                                        <table>
                                            <thead>
                                                <tr>
                                                    <th>ID</th>
                                                    <th>Status</th>
                                                    <th>Price</th>
                                                    <th>Address</th>
                                                    <th>City</th>
                                                    <th>Beds/Baths</th>
                                                    <th>SqFt</th>
                                                </tr>
                                            </thead>
                                            <tbody id="ui-data-table">
                                                <tr>
                                                    <td colspan="7" class="empty-state">Loading records...</td>
                                                </tr>
                                            </tbody>
                                        </table>
                                    `, { html: true });
                                }
                            }
                        })
                        .transform(res);
                } catch (e) {
                    return new Response(e.message, { status: 500 });
                }
            }
        }

        return new Response('Not Found', { status: 404 });
    },
    
    async scheduled(event, env, ctx) {
        // Runs automatically via Cloudflare Cron Triggers (Heartbeat: Hourly)
        try {
            const { results } = await env.mls_data.prepare("SELECT * FROM boards").all();
            for (const board of results) {
                console.log(`Cron: Running scheduled ETL for board ${board.id}`);
                await runExtraction(board.id, env);
            }
        } catch (e) {
            console.error('CRON ERROR:', e);
        }
    }
};
