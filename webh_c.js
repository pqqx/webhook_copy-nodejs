const axios = require('axios');
const config = require('./webh_c.json');
const operatorApi = axios.create({ baseURL: 'https://discord.com/api/v10', headers: { 'Authorization': config.operatorToken, 'Content-Type': 'application/json' }});
const sourceApi = axios.create({ baseURL: 'https://discord.com/api/v10', headers: { 'Authorization': config.messageToken, 'Content-Type': 'application/json' }});

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function apiRequestWithRetry(apiInstance, requestFunction) {
    try {
        return await requestFunction(apiInstance);
    } catch (error) {
        if (error.response?.status === 429) {
            const retryAfter = error.response.data.retry_after * 1000 + 500;
            console.warn(`Rate limited. Retrying after ${retryAfter} ms...`);
            await sleep(retryAfter);
            return apiRequestWithRetry(apiInstance, requestFunction);
        }
        console.error('API Request failed:', error.response?.data || error.message);
        throw error;
    }
}

async function syncServerChannels() {
    console.log('Starting channel synchronization...');
    try {
        const [sourceChannels, targetChannels] = await Promise.all([
            apiRequestWithRetry(sourceApi, api => api.get(`/guilds/${config.sourceGuildId}/channels`)).then(res => res.data),
            apiRequestWithRetry(operatorApi, api => api.get(`/guilds/${config.targetGuildId}/channels`)).then(res => res.data)
        ]);
        console.log(`Source server has ${sourceChannels.length} channels, Target server has ${targetChannels.length} channels.`);
        const sourceMap = new Map(sourceChannels.map(c => [c.id, c]));
        const targetMap = new Map(targetChannels.map(c => [c.id, c]));

        const getCategoryName = (channel, channelMap) => channel.parent_id ? channelMap.get(channel.parent_id)?.name : null;

        const targetStructure = new Map();
        for (const tc of targetChannels) {
            if (tc.type === 4) continue; // カテゴリは別途処理
            const categoryName = getCategoryName(tc, targetMap);
            if (!targetStructure.has(categoryName)) {
                targetStructure.set(categoryName, new Set());
            }
            targetStructure.get(categoryName).add(tc.name);
        }

        const sourceCategories = sourceChannels.filter(c => c.type === 4);
        const targetCategoryNames = new Set(targetChannels.filter(c => c.type === 4).map(c => c.name));

        for (const sourceCategory of sourceCategories) {
            if (!targetCategoryNames.has(sourceCategory.name)) {
                console.log(`Creating category: "${sourceCategory.name}"`);
                const newCategory = (await apiRequestWithRetry(operatorApi, api =>
                    api.post(`/guilds/${config.targetGuildId}/channels`, {
                        name: sourceCategory.name,
                        type: 4, 
                        permission_overwrites: sourceCategory.permission_overwrites,
                        position: sourceCategory.position // <-- 修正点：カテゴリの位置情報を追加
                    })
                )).data;
                targetMap.set(newCategory.id, newCategory);
                targetStructure.set(newCategory.name, new Set());
            }
        }
        
        // ターゲットのカテゴリ名とIDのマップを再作成
        const targetCategoryMap = new Map(Array.from(targetMap.values()).filter(c => c.type === 4).map(c => [c.name, c.id]));

        for (const sourceChannel of sourceChannels) {
            // 対象とするチャンネルタイプを限定 (テキスト, ボイス, アナウンス, フォーラムなど)
            if (![0, 2, 5, 15].includes(sourceChannel.type)) continue;

            const sourceCategoryName = getCategoryName(sourceChannel, sourceMap);
            const targetChannelsInCategory = targetStructure.get(sourceCategoryName);
            if (!targetChannelsInCategory || !targetChannelsInCategory.has(sourceChannel.name)) {
                const parentId = sourceCategoryName ? targetCategoryMap.get(sourceCategoryName) : null;
                console.log(`Creating channel: #${sourceChannel.name}` + (sourceCategoryName ? ` in category "${sourceCategoryName}"` : ''));

                await apiRequestWithRetry(operatorApi, api =>
                    api.post(`/guilds/${config.targetGuildId}/channels`, {
                        name: sourceChannel.name,
                        type: sourceChannel.type,
                        topic: sourceChannel.topic,
                        parent_id: parentId,
                        permission_overwrites: sourceChannel.permission_overwrites,
                        position: sourceChannel.position
                    })
                );
            }
        }

        console.log('Channel synchronization finished successfully!');

    } catch (error) {
        console.error('A critical error occurred during channel synchronization.');
    }
}

syncServerChannels();