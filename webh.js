const { Client, GatewayIntentBits, SlashCommandBuilder, AttachmentBuilder } = require('discord.js');
const WebSocket = require('ws');
const axios = require('axios');
const fs = require('fs');
const crypto = require('crypto');
const path = require('path');
const FormData = require('form-data');

const logsDir = path.join(__dirname, 'logs');
const MAX_LOG_FILES = 10;
if (!fs.existsSync(logsDir)) { fs.mkdirSync(logsDir); }
try {
    const logFiles = fs.readdirSync(logsDir).filter(f => f.endsWith('-bot.log')).map(f => ({ name: f, time: fs.statSync(path.join(logsDir, f)).mtime.getTime() })).sort((a, b) => a.time - b.time);
    if (logFiles.length > MAX_LOG_FILES) {
        const filesToDelete = logFiles.slice(0, logFiles.length - MAX_LOG_FILES);
        console.log(`[LogManager] Found ${logFiles.length} log files. Deleting ${filesToDelete.length} oldest ones...`);
        filesToDelete.forEach(f => fs.unlinkSync(path.join(logsDir, f.name)));
    }
} catch (e) { console.error('[LogManager] Failed to clean up old log files:', e); }
const logFilePrefix = crypto.randomBytes(4).toString('hex');
const logFileName = `${logFilePrefix}-bot.log`;
const logFilePath = path.join(logsDir, logFileName);
const logStream = fs.createWriteStream(logFilePath, { flags: 'a' });
const logToFile = (message) => { const logMessage = `[${new Date().toISOString()}] ${message}\n`; logStream.write(logMessage); process.stdout.write(logMessage); };
console.log = (message) => logToFile(message);
console.error = (message, ...optionalParams) => { const fullMessage = [message, ...optionalParams].map(p => typeof p === 'object' ? JSON.stringify(p, null, 2) : p).join(' '); logToFile(`ERROR: ${fullMessage}`); };
console.warn = (message) => logToFile(`WARN: ${message}`);

let config = require('./webh.json');
let MAPPINGS = {};
let messageMap = new Map();
const CACHE_LIMIT = 500;
let ws, interval, syncDebounceTimer;
let sourceApi, operatorApi;
let proxyList = [];
let sourceSystemChannelId = null;

try {
    const proxyPath = path.join(__dirname, 'proxies.txt');
    if (fs.existsSync(proxyPath)) {
        proxyList = fs.readFileSync(proxyPath, 'utf-8')
            .split('\n')
            .map(l => l.trim())
            .filter(l => l && !l.startsWith('#'));
        console.log(`[Proxy] Loaded ${proxyList.length} Proxies.`);
    } else {
        console.log('[Proxy] Not found.');
    }
} catch (e) {
    console.error('[Proxy] Proxy Load Error:', e);
}

function parseProxy(proxyStr) {
    if (!proxyStr) return false;
    const parts = proxyStr.split(':');
    if (parts.length === 4) {
        return {
            protocol: 'http',
            host: parts[0],
            port: parseInt(parts[1], 10),
            auth: { username: parts[2], password: parts[3] }
        };
    } else if (parts.length === 2) {
        return {
            protocol: 'http',
            host: parts[0],
            port: parseInt(parts[1], 10)
        };
    }
    return false;
}

function getRandomProxyConfig() {
    if (proxyList.length === 0) return false;
    const randomIndex = Math.floor(Math.random() * proxyList.length);
    const proxyStr = proxyList[randomIndex];
    return parseProxy(proxyStr);
}

function updateApiInstances() {
    const axiosConfig = {
        baseURL: 'https://discord.com/api/v9',
        proxy: false,
        headers: { 'Content-Type': 'application/json' },
        validateStatus: function (status) {
            return status >= 200 && status < 300; 
        }
    };

    sourceApi = axios.create({ ...axiosConfig, headers: { ...axiosConfig.headers, 'Authorization': config.messageToken } });
    operatorApi = axios.create({ ...axiosConfig, headers: { ...axiosConfig.headers, 'Authorization': config.operatorToken } });
    
    console.log(`[API] Channel Mapping was Updated.`);
}

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function apiRequestWithRetry(apiInstance, requestFunction) {
    try {
        return await requestFunction(apiInstance);
    }
    catch (error) {
        const status = error.response?.status;
        
        if (status === 429) {
            const retryAfter = (error.response.data.retry_after * 1000) + 500 || 5000;
            console.warn(`[API] Rate limited. Retrying after ${retryAfter} ms...`);
            await sleep(retryAfter);
            return apiRequestWithRetry(apiInstance, requestFunction);
        }

        const isBlockedStatus = [403, 502, 503].includes(status);
        if (isBlockedStatus) {
            console.error(`[API] Blocked (${status}). Waiting 30s.`);
            await sleep(30000);
            return apiRequestWithRetry(apiInstance, requestFunction);
        }

        console.error('API Request failed:', error.response?.data || error.message);
        throw error;
    }
}

async function setupAndSyncMappings() {
    console.log('checking channels...');
    const newMappings = {};
    const loggableMappings = [];
    try {
        const [sourceGuildData, sourceChannels, targetChannels] = await Promise.all([
            apiRequestWithRetry(sourceApi, api => api.get(`/guilds/${config.sourceGuildId}`)).then(res => res.data),
            apiRequestWithRetry(sourceApi, api => api.get(`/guilds/${config.sourceGuildId}/channels`)).then(res => res.data),
            apiRequestWithRetry(operatorApi, api => api.get(`/guilds/${config.targetGuildId}/channels`)).then(res => res.data)
        ]);

        sourceSystemChannelId = sourceGuildData.system_channel_id;

        const textBasedChannelTypes = [0, 2, 5, 15];
        for (const [sourceId, targetId] of Object.entries(config.channelIdOverrides)) {
            const source = sourceChannels.find(c => c.id === sourceId), target = targetChannels.find(c => c.id === targetId);
            if (source && target) {
                const url = await getOrCreateWebhook(target);
                if (url) { newMappings[sourceId] = url; loggableMappings.push(`OVERRIDE: #${source.name} (${source.id}) -> #${target.name} (${target.id})`); }
            }
        }
        const sourceChannelMap = new Map(sourceChannels.map(c => [c.id, c]));
        for (const sourceChannel of sourceChannels) {
            if (newMappings[sourceChannel.id] || !textBasedChannelTypes.includes(sourceChannel.type)) continue;
            const sourceCategory = sourceChannel.parent_id ? sourceChannelMap.get(sourceChannel.parent_id) : null;
            const targetChannel = targetChannels.find(tc => {
                const targetCategory = tc.parent_id ? new Map(targetChannels.map(c => [c.id, c])).get(tc.parent_id) : null;
                return tc.name === sourceChannel.name && (sourceCategory?.name === targetCategory?.name);
            });
            if (targetChannel) {
                const url = await getOrCreateWebhook(targetChannel);
                if (url) { newMappings[sourceChannel.id] = url; loggableMappings.push(`AUTO-MAP: #${sourceChannel.name} (${sourceChannel.id}) -> #${targetChannel.name} (${targetChannel.id})`); }
            }
        }
        MAPPINGS = newMappings;
        console.log(`\n${Object.keys(MAPPINGS).length} channels mapped.`);
        if (loggableMappings.length > 0) { console.log('- Mappings'); loggableMappings.forEach(line => console.log(line)); console.log('-'); }
        else { console.log('No active mappings not found'); }
    } catch (error) { console.error('A critical error occurred during mapping sync.'); }
}

async function getOrCreateWebhook(channel) {
    try {
        const res = await apiRequestWithRetry(operatorApi, api => api.get(`/channels/${channel.id}/webhooks`));
        let webhook = res.data.find(wh => wh.name === 'MirrorForwarder');
        if (!webhook) { webhook = (await apiRequestWithRetry(operatorApi, api => api.post(`/channels/${channel.id}/webhooks`, { name: 'MirrorForwarder' }))).data; }
        return webhook.url;
    } catch { return null; }
}

function triggerMappingSync() {
    clearTimeout(syncDebounceTimer);
    console.log('Channel change detected. Re-Syncing...');
    syncDebounceTimer = setTimeout(setupAndSyncMappings, 1000);
}

const payload = { 
    op: 2, 
    d: { 
        token: config.messageToken, 
        intents: 33283, 
        properties: { 
            $os: 'windows', 
            $browser: 'chrome', 
            $device: 'pc' 
        }, 
        compress: false, 
        large_threshold: 250 
    }
};

function connect() {
    if (ws && ws.readyState === WebSocket.OPEN) { ws.close(); }
    ws = new WebSocket('wss://gateway.discord.gg/?v=9&encoding=json');
    ws.on('open', () => { console.log('Connected to WebSocket'); ws.send(JSON.stringify(payload)); });
    ws.on('message', async (data) => {
        const { op, t, d } = JSON.parse(data);
        if (op === 0) {
            switch (t) {
                case 'MESSAGE_CREATE': await handleMessageCreate(d); break;
                case 'MESSAGE_UPDATE': await handleMessageUpdate(d); break;
                case 'MESSAGE_DELETE': await handleMessageDelete(d); break;
                
                case 'GUILD_MEMBER_ADD':
                    if (d.guild_id === config.sourceGuildId && sourceSystemChannelId) {
                        const webhookUrl = MAPPINGS[sourceSystemChannelId];
                        if (webhookUrl) {
                            try {
                                const displayName = d.nick || d.user.global_name || d.user.username;
                                const avatarUrl = getAvatarUrl(d.user, d, d.guild_id);
                                
                                // ディスプレイネーム (username) の形式を作成
                                let webhookName = `${displayName} (${d.user.username})`;
                                if (webhookName.length > 80) webhookName = webhookName.substring(0, 80);

                                await axios.post(webhookUrl, 
                                    { 
                                        content: `**${d.user.username} (${d.user.id})** さんがサーバーに参加しました。`,
                                        username: webhookName,
                                        avatar_url: avatarUrl
                                    }, 
                                    { proxy: getRandomProxyConfig() }
                                );
                            } catch (error) {
                                console.error('Failed to send Join Log:', error.message);
                            }
                        }
                    }
                    break;

                case 'CHANNEL_CREATE': case 'CHANNEL_DELETE': case 'CHANNEL_UPDATE':
                    if (d.guild_id === config.sourceGuildId || d.guild_id === config.targetGuildId) { triggerMappingSync(); }
                    break;
            }
        } else if (op === 10) { const { heartbeat_interval } = d; if(interval) clearInterval(interval); interval = setInterval(() => ws.send(JSON.stringify({ op: 1, d: null })), heartbeat_interval); }
        else if (op === 7) { console.log('Reconnect required by Discord.'); closeAndReconnect(); }
    });
    ws.on('close', (code) => { console.log(`WebSocket closed: ${code}.`); closeAndReconnect(); });
    ws.on('error', (err) => { console.error('WebSocket Error:', err.message); });
}

function getFormattedUsername(author, member) {
  const name = member?.nick || author.global_name || author.username;
  const formatted = `${name} (${author.username})`.substring(0, 80);
  return formatted.replace(/discord|blank grabber/gi, (match) => {
    return '***'.repeat(match.length);
  });
}

function getAvatarUrl(author, member, guildId) {
    const memberAvatar = member?.avatar;
    if (memberAvatar && guildId) {
        return `https://cdn.discordapp.com/guilds/${guildId}/users/${author.id}/avatars/${memberAvatar}.png`;
    }
    if (author.avatar) {
        return `https://cdn.discordapp.com/avatars/${author.id}/${author.avatar}.png`;
    }
    const index = Number((BigInt(author.id) >> 22n) % 6n);
    return `https://cdn.discordapp.com/embed/avatars/${index}.png`;
}

function formatMessageParts(d) {
    const replyParts = [];
    const contentParts = [];

    if (d.referenced_message) {
        const rm = d.referenced_message;
        const url = `https://discord.com/channels/${config.sourceGuildId}/${rm.channel_id}/${rm.id}`;
        let rc = (rm.content || '...').replace(/\n/g, ' ').substring(0, 30);
        if (rc.length >= 30) rc += '...';
        replyParts.push(`-# |-- @${rm.author.username} [${rc}](${url})`);
    }

    if (d.content) contentParts.push(d.content);
    if (d.sticker_items?.length > 0) contentParts.push(...d.sticker_items.map(s => `https://cdn.discordapp.com/stickers/${s.id}.png`));
    
    return {
        replyPart: replyParts.join('\n') || null,
        contentPart: contentParts.join('\n') || null,
        embeds: d.embeds || []
    };
}

async function handleMessageCreate(d) {
    if (d.webhook_id || d.guild_id !== config.sourceGuildId) return;
    const webhookUrl = MAPPINGS[d.channel_id];
    if (!webhookUrl) return;

    try {
        const username = getFormattedUsername(d.author, d.member);
        const avatar_url = getAvatarUrl(d.author, d.member, d.guild_id); 

        if (d.message_snapshots && d.message_snapshots.length > 0) {
            const embeds = [];
            for (const snapshot of d.message_snapshots) {
                const message = snapshot.message;
                if (!message) continue;

                let description = message.content || ''; 
                
                if (message.attachments && message.attachments.length > 0) {
                    description += '\n\n';
                    message.attachments.forEach(att => {
                        description += `[ファイル: ${att.filename}](${att.url})\n`;
                    });
                }

                if (!description) description = '\u200b';

                if (embeds.length < 10) {
                    embeds.push({
                        description: description.substring(0, 4096),
                        author: {
                            name: '転送済みメッセージ'
                        }
                    });
                }
            }

            const content = d.content || '';

            if (embeds.length > 0) {
                await axios.post(webhookUrl, 
                    { 
                        username, 
                        avatar_url, 
                        content, 
                        embeds, 
                        allowed_mentions: { parse: [] } 
                    },
                    { proxy: getRandomProxyConfig() } 
                );
            }
        } else {
            const { replyPart, contentPart, embeds } = formatMessageParts(d);

            const FILE_SIZE_LIMIT = 9.9 * 1024 * 1024;
            const filesToUpload = [], largeFileLinks = [];
            if (d.attachments && d.attachments.length > 0) {
                for (const attachment of d.attachments) {
                    if (attachment.size < FILE_SIZE_LIMIT) filesToUpload.push(attachment);
                    else largeFileLinks.push(`\n[ファイル(大容量のためリンク): ${attachment.filename}](${attachment.url})`);
                }
            }

            const textContent = [replyPart, contentPart].filter(Boolean).join('\n');
            const finalContent = [textContent, ...largeFileLinks].filter(Boolean).join('\n');

            if (!finalContent && filesToUpload.length === 0 && embeds.length === 0) return;
            
            let res;
            
            
            if (filesToUpload.length > 0) {
                const formData = new FormData();
                const filePromises = filesToUpload.map(async (att) => {
                    const response = await axios.get(att.url, { responseType: 'arraybuffer', proxy: false }); 
                    return { buffer: response.data, filename: att.filename };
                });
                const files = await Promise.all(filePromises);
                files.forEach((file, index) => formData.append(`file[${index}]`, file.buffer, file.filename));
                const payload = { content: finalContent, username, avatar_url, embeds, allowed_mentions: { parse: [] } };
                formData.append('payload_json', JSON.stringify(payload));
                
                res = await axios.post(`${webhookUrl}?wait=true`, formData, { 
                    headers: formData.getHeaders(),
                    proxy: false 
                });
            } else {
                res = await axios.post(`${webhookUrl}?wait=true`, 
                    { content: finalContent || '\u200b', username, avatar_url, embeds, allowed_mentions: { parse: [] } },
                    { proxy: getRandomProxyConfig() }
                );
            }
            
            if (res) {
                 messageMap.set(d.id, {
                    targetMessageId: res.data.id,
                    webhookUrl: webhookUrl,
                    replyPart: replyPart,
                    currentContent: contentPart,
                    embeds: embeds
                });
            }
        }
    } catch (error) {
        if (error.response?.data && typeof error.response.data === 'string' && error.response.data.includes('cf-error')) {
            console.warn('[WebHook] Cf nigger detected.');
        } else {
            console.error(`Message Send Error: ${error.response?.status || 'Unknown'}`, error.response?.data || error.message);
        }
    }
}

async function handleMessageUpdate(d) {
    if (!messageMap.has(d.id)) return;
    const cached = messageMap.get(d.id);
    const { contentPart: newContentPart, embeds } = formatMessageParts(d);

    const newContent = [
        cached.replyPart,
        `${cached.currentContent} (edited)`,
        newContentPart
    ].filter(Boolean).join('\n');
    if (newContent.length > 2000) return;

    try {
        await axios.patch(`${cached.webhookUrl}/messages/${cached.targetMessageId}`, 
            { content: newContent, embeds }, 
            { proxy: getRandomProxyConfig() }
        );
        cached.currentContent = `${cached.currentContent} (edited)\n${newContentPart}`;
        cached.embeds = embeds;
        messageMap.set(d.id, cached);
    } catch (error) { 
        console.error(`UPDATE Error:`, error.response?.data?.message || error.message); 
    }
}

async function handleMessageDelete(d) {
    if (!messageMap.has(d.id)) return;
    const cached = messageMap.get(d.id);
    const deletedContent = [
        cached.replyPart,
        `${cached.currentContent} (deleted)`
    ].filter(Boolean).join('\n');
    if (deletedContent.length > 2000) return;
    try {
        await axios.patch(`${cached.webhookUrl}/messages/${cached.targetMessageId}`, 
            { content: deletedContent, embeds: cached.embeds }, 
            { proxy: getRandomProxyConfig() }
        );
        messageMap.delete(d.id);
    } catch (error) { messageMap.delete(d.id); }
}

async function closeAndReconnect() {
    if (interval) clearInterval(interval);
    if (ws) { ws.removeAllListeners(); ws.close(); }
    console.log('--- Reconnecting. Re-syncing mappings... ---');
    await setupAndSyncMappings();
    setTimeout(connect, 5000);
}
async function startMirroring() {
    updateApiInstances();
    await setupAndSyncMappings();
    connect();
}

const client = new Client({ intents: [GatewayIntentBits.Guilds] });
const commands = [
    new SlashCommandBuilder().setName('changejson').setDescription('webh.jsonの設定値を変更します。').addStringOption(option => option.setName('msgtoken').setDescription('新しいMessage Token')).addStringOption(option => option.setName('optoken').setDescription('新しいOperator Token')).addStringOption(option => option.setName('sourceserverid').setDescription('新しいSource Server ID')).addStringOption(option => option.setName('targetserverid').setDescription('新しいTarget Server ID')),
    new SlashCommandBuilder().setName('outputlog').setDescription('現在のログファイルを添付します。'),
    new SlashCommandBuilder().setName('restart').setDescription('ボットを再起動し、設定を再同期します。')
];
// client.once('clientReady', async () => {
//    console.log(`Logged in as ${client.user.tag}!`);
//    try {
//        console.log('Registering slash commands...');
//        await client.application.commands.set(commands);
//        console.log('Slash commands registered successfully.');
//    } catch (error) { console.error('Failed to register slash commands:', error); }
//    startMirroring();
// });

client.on('interactionCreate', async interaction => {
    if (!interaction.isCommand()) return;
    if (!config.allowedUserIds || !config.allowedUserIds.includes(interaction.user.id)) {
        await interaction.reply({ content: 'このコマンドを使用する権限がありません。', ephemeral: true });
        return;
    }
    const { commandName } = interaction;
    if (commandName === 'changejson') {
        await interaction.deferReply({ ephemeral: true });
        try {
            const newConfig = { ...config };
            const msgToken = interaction.options.getString('msgtoken');
            const opToken = interaction.options.getString('optoken');
            const sourceId = interaction.options.getString('sourceserverid');
            const targetId = interaction.options.getString('targetserverid');
            let changed = false;
            if (msgToken) { newConfig.messageToken = msgToken; changed = true; }
            if (opToken) { newConfig.operatorToken = opToken; changed = true; }
            if (sourceId) { newConfig.sourceGuildId = sourceId; changed = true; }
            if (targetId) { newConfig.targetGuildId = targetId; changed = true; }
            if (changed) {
                fs.writeFileSync('./webh.json', JSON.stringify(newConfig, null, 4));
                config = newConfig;
                updateApiInstances();
                await interaction.editReply('設定を更新しました。5秒後にマッピングを再同期します。');
                triggerMappingSync();
            } else {
                await interaction.editReply('変更するオプションが指定されていません。');
            }
        } catch (error) {
            console.error('Failed to update config:', error);
            await interaction.editReply('設定の更新中にエラーが発生しました。');
        }
    } else if (commandName === 'outputlog') {
        await interaction.deferReply();
        try {
            const attachment = new AttachmentBuilder(logFilePath);
            await interaction.editReply({ content: `現在のログファイル(\`${logFileName}\`)です:`, files: [attachment] });
        } catch (error) {
            console.error('Failed to send log file:', error);
            await interaction.editReply('ログファイルの送信に失敗しました。');
        }
    } else if (commandName === 'restart') {
        await interaction.reply({ content: '再起動します...', ephemeral: true });
        console.log('--- Restarting by command ---');
        if (ws) ws.close();
        if (interval) clearInterval(interval);
        setTimeout(() => process.exit(0), 1000);
    }
});

// client.login(config.botToken);
startMirroring();