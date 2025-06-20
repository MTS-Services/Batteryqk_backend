import prisma from "../utils/prismaClient.js";
import { recordAuditLog } from "../utils/auditLogHandler.js";
import { AuditLogAction } from "@prisma/client";
import { createClient } from "redis";
import * as deepl from "deepl-node";

// --- DeepL Configuration ---
const DEEPL_AUTH_KEY = process.env.DEEPL_AUTH_KEY || "YOUR_DEEPL_AUTH_KEY_HERE";
if (DEEPL_AUTH_KEY === "YOUR_DEEPL_AUTH_KEY_HERE") {
    console.warn("DeepL Auth Key is a placeholder. AR translations may not work. Please configure process.env.DEEPL_AUTH_KEY.");
}
const deeplClient = DEEPL_AUTH_KEY !== "YOUR_DEEPL_AUTH_KEY_HERE" ? new deepl.Translator(DEEPL_AUTH_KEY) : null;

// --- Redis Configuration ---
const REDIS_URL = process.env.REDIS_URL || "redis://default:YOUR_REDIS_PASSWORD@YOUR_REDIS_HOST:PORT";
if (REDIS_URL.includes("YOUR_REDIS_PASSWORD") || REDIS_URL.includes("YOUR_REDIS_HOST")) {
    console.warn("Redis URL seems to contain placeholder values. Please configure process.env.REDIS_URL for AR caching.");
}
const AR_CACHE_EXPIRATION = 365 * 24 * 60 * 60; // 365 days in seconds

const redisClient = createClient({
    url: REDIS_URL,
    socket: {
        reconnectStrategy: (retries) => {
            console.log(`Redis: AR Category Cache - Attempting to reconnect. Retry: ${retries + 1}`);
            if (retries >= 3) {
                console.error("Redis: AR Category Cache - Max reconnect retries reached. Stopping retries.");
                return false;
            }
            return Math.min(retries * 200, 5000);
        },
    },
});

redisClient.on('connecting', () => console.log('Redis: AR Category Cache - Connecting...'));
redisClient.on('ready', () => console.log('Redis: AR Category Cache - Client is ready.'));
redisClient.on('error', (err) => console.error('Redis: AR Category Cache - Client Error ->', err.message));
redisClient.on('end', () => console.log('Redis: AR Category Cache - Connection ended.'));

(async () => {
    try {
        await redisClient.connect();
    } catch (err) {
        console.error('Redis: AR Category Cache - Could not connect on initial attempt ->', err.message);
    }
})();

const cacheKeys = {
    categoryAr: (mainId, subId) => `category:${mainId}:${subId}:ar`,
    allCategoriesAr: () => `categories:all_formatted:ar`,
};

// --- Helper Functions ---
async function translateText(text, targetLang, sourceLang = null) {
    if (!deeplClient || !text || typeof text !== 'string') {
        return text;
    }
    try {
        const result = await deeplClient.translateText(text, sourceLang, targetLang);
        return result.text;
    } catch (error) {
        console.error(`DeepL Translation error: ${error.message}. Text: "${text}", TargetLang: ${targetLang}, SourceLang: ${sourceLang || 'auto'}`);
        return text;
    }
}

// --- Category Service ---
const categoryService = {
    async createCategory(data, lang = "en", reqDetails = {}) {
        const { mainCategory, subCategories, mainCategoryId } = data;
        
        let mainCategoryName = mainCategory;
        let originalMainCategory = mainCategory;
        let originalSubCategories = subCategories;

        // Use existing main category if ID provided, otherwise create or find by name
        let mainCategoryRecord;
        if (mainCategoryId) {
            mainCategoryRecord = await prisma.mainCategoryOption.findUnique({
                where: { id: parseInt(mainCategoryId, 10) }
            });
            
            if (!mainCategoryRecord) {
                throw new Error("Main category with provided ID not found.");
            }
            
            // Set the main category name from existing record
            mainCategoryName = mainCategoryRecord.name;
            originalMainCategory = mainCategoryRecord.name;
        } else {
            if (lang === "ar") {
                // Convert Arabic to English for database storage
                mainCategoryName = await translateText(mainCategory, "EN-US", "AR");
            }

            mainCategoryRecord = await prisma.mainCategoryOption.findFirst({
                where: { name: mainCategoryName }
            });
            
            if (!mainCategoryRecord) {
                mainCategoryRecord = await prisma.mainCategoryOption.create({
                    data: { name: mainCategoryName }
                });
            }
        }

        // Prepare immediate response
        const immediateResponse = {
            success: true,
            message: "Category creation initiated. Processing in background.",
            mainCategory: {
                name: lang === "ar" ? (originalMainCategory || mainCategoryRecord.name) : mainCategoryRecord.name,
                id: mainCategoryRecord.id
            },
            status: "processing"
        };

        // Process subcategories and specific items in background
        setImmediate(async () => {
            try {
                const createdSubCategories = [];

                // Handle multiple subcategories
                for (const subCatData of subCategories) {
                    let subCategoryName = subCatData.name;
                    let specificItemNames = subCatData.specificItems || [];

                    if (lang === "ar") {
                        // Convert Arabic to English for database storage
                        subCategoryName = await translateText(subCatData.name, "EN-US", "AR");
                        specificItemNames = await Promise.all(
                            subCatData.specificItems.map(item => translateText(item, "EN-US", "AR"))
                        );
                    }

                    // Create or find sub category
                    let subCategoryRecord = await prisma.subCategoryOption.findFirst({
                        where: { 
                            name: subCategoryName,
                            mainCategoryId: mainCategoryRecord.id 
                        }
                    });
                    
                    if (!subCategoryRecord) {
                        subCategoryRecord = await prisma.subCategoryOption.create({
                            data: { 
                                name: subCategoryName,
                                mainCategoryId: mainCategoryRecord.id 
                            }
                        });
                    }

                    // Create specific items for this subcategory
                    const specificItemRecords = [];
                    for (const itemName of specificItemNames) {
                        let specificItemRecord = await prisma.specificItemOption.findFirst({
                            where: { 
                                name: itemName,
                                subCategoryId: subCategoryRecord.id,
                                mainCategoryId: mainCategoryRecord.id
                            }
                        });
                        
                        if (!specificItemRecord) {
                            specificItemRecord = await prisma.specificItemOption.create({
                                data: { 
                                    name: itemName,
                                    subCategoryId: subCategoryRecord.id,
                                    mainCategoryId: mainCategoryRecord.id
                                }
                            });
                        }
                        specificItemRecords.push(specificItemRecord);
                    }

                    createdSubCategories.push({
                        record: subCategoryRecord,
                        specificItems: specificItemRecords,
                        originalData: subCatData
                    });

                    // Cache Arabic version in Redis for each subcategory
                    try {
                        if (lang === "ar") {
                            // Store original Arabic input in cache
                            const cacheData = {
                                mainCategory: { name: originalMainCategory, id: mainCategoryRecord.id },
                                subCategories: { name: subCatData.name, id: subCategoryRecord.id },
                                specificItem: specificItemRecords.map((item, index) => ({
                                    name: subCatData.specificItems[index],
                                    id: item.id
                                })),
                                createdAt: mainCategoryRecord.createdAt,
                                updatedAt: mainCategoryRecord.updatedAt
                            };
                            await redisClient.setEx(
                                cacheKeys.categoryAr(mainCategoryRecord.id, subCategoryRecord.id),
                                AR_CACHE_EXPIRATION,
                                JSON.stringify(cacheData)
                            );
                        } else {
                            // Convert to Arabic and store in cache
                            const arMainCategory = await translateText(mainCategoryRecord.name, "AR", "EN");
                            const arSubCategory = await translateText(subCategoryRecord.name, "AR", "EN");
                            const arSpecificItems = await Promise.all(
                                specificItemRecords.map(async (item) => ({
                                    name: await translateText(item.name, "AR", "EN"),
                                    id: item.id
                                }))
                            );
                            
                            const cacheData = {
                                mainCategory: { name: arMainCategory, id: mainCategoryRecord.id },
                                subCategories: { name: arSubCategory, id: subCategoryRecord.id },
                                specificItem: arSpecificItems,
                                createdAt: mainCategoryRecord.createdAt,
                                updatedAt: mainCategoryRecord.updatedAt
                            };
                            await redisClient.setEx(
                                cacheKeys.categoryAr(mainCategoryRecord.id, subCategoryRecord.id),
                                AR_CACHE_EXPIRATION,
                                JSON.stringify(cacheData)
                            );
                        }
                    } catch (error) {
                        console.error('Redis cache error during category creation:', error.message);
                    }
                }

             
                try {
                    await redisClient.del(cacheKeys.allCategoriesAr());
                } catch (error) {
                    console.error('Redis cache error during cache invalidation:', error.message);
                }

                // Record audit log
                recordAuditLog(AuditLogAction.CATEGORY_CREATED, {
                    userId: reqDetails.actorUserId,
                    entityName: 'Category',
                    entityId: mainCategoryRecord.id.toString(),
                    newValues: { mainCategoryRecord, subCategories: createdSubCategories },
                    description: mainCategoryId 
                        ? `Subcategories added to existing category '${mainCategoryRecord.name}' with ${createdSubCategories.length} subcategories.`
                        : `Category '${mainCategoryName}' with ${createdSubCategories.length} subcategories created.`,
                    ipAddress: reqDetails.ipAddress,
                    userAgent: reqDetails.userAgent,
                });

                console.log(`Background processing completed for category: ${mainCategoryRecord.name}`);
            } catch (error) {
                console.error('Background category processing error:', error.message);
            }
        });

        return immediateResponse;
    },

    async getAllCategories(lang = "en") {
        if (lang === "ar") {
            try {
                // Try to get from Redis cache first
                const cachedData = await redisClient.get(cacheKeys.allCategoriesAr());
                if (cachedData) {
                    console.log('Redis cache hit for all categories in Arabic');
                    return JSON.parse(cachedData);
                }
            } catch (error) {
                console.error('Redis cache error during getAllCategories:', error.message);
            }

            // If not in cache, fetch from DB
            const mainCategories = await prisma.mainCategoryOption.findMany({
                include: {
                    subCategories: {
                        include: {
                            specificItems: true
                        }
                    }
                },
                orderBy: { id: 'desc' }
            });

            const formattedCategories = [];

            for (const mainCat of mainCategories) {
                const mainCategoryName = await translateText(mainCat.name, "AR", "EN");
                const subCategoriesData = [];

                for (const subCat of mainCat.subCategories) {
                    const subCategoryName = await translateText(subCat.name, "AR", "EN");
                    const specificItems = await Promise.all(
                        subCat.specificItems.map(async (item) => ({
                            name: await translateText(item.name, "AR", "EN"),
                            id: item.id
                        }))
                    );

                    subCategoriesData.push({
                        name: subCategoryName,
                        id: subCat.id,
                        specificItems: specificItems
                    });
                }

                formattedCategories.push({
                    mainCategory: {
                        name: mainCategoryName,
                        id: mainCat.id
                    },
                    subCategories: subCategoriesData,
                    createdAt: mainCat.createdAt,
                    updatedAt: mainCat.updatedAt
                });
            }

            // Store in Redis cache
            try {
                console.log('Storing all categories in Redis cache for Arabic');
                await redisClient.setEx(
                    cacheKeys.allCategoriesAr(),
                    AR_CACHE_EXPIRATION,
                    JSON.stringify(formattedCategories)
                );
            } catch (error) {
                console.error('Redis cache error during getAllCategories storage:', error.message);
            }

            return formattedCategories;
        } else {
            // English - fetch directly from DB
            const mainCategories = await prisma.mainCategoryOption.findMany({
                include: {
                    subCategories: {
                        include: {
                            specificItems: true
                        }
                    }
                },
                orderBy: { id: 'desc' }
            });

            const formattedCategories = [];

            for (const mainCat of mainCategories) {
                const subCategoriesData = [];

                for (const subCat of mainCat.subCategories) {
                    subCategoriesData.push({
                        name: subCat.name,
                        id: subCat.id,
                        specificItems: subCat.specificItems.map(item => ({
                            name: item.name,
                            id: item.id
                        }))
                    });
                }

                formattedCategories.push({
                    mainCategory: {
                        name: mainCat.name,
                        id: mainCat.id
                    },
                    subCategories: subCategoriesData,
                    createdAt: mainCat.createdAt,
                    updatedAt: mainCat.updatedAt
                });
            }

            return formattedCategories;
        }
    },

    async getCategoryById(id, lang = "en") {
        const mainCategoryId = parseInt(id, 10);
        if (isNaN(mainCategoryId)) {
            throw new Error("Invalid category ID format.");
        }

        const mainCategory = await prisma.mainCategoryOption.findUnique({
            where: { id: mainCategoryId },
            include: {
                subCategories: {
                    include: {
                        specificItems: true
                    }
                }
            }
        });

        if (!mainCategory) return null;

        if (lang === "ar") {
            const mainCategoryName = await translateText(mainCategory.name, "AR", "EN");
            const subCategoriesData = [];

            for (const subCat of mainCategory.subCategories) {
                const subCategoryName = await translateText(subCat.name, "AR", "EN");
                const specificItems = await Promise.all(
                    subCat.specificItems.map(async (item) => ({
                        name: await translateText(item.name, "AR", "EN"),
                        id: item.id
                    }))
                );

                subCategoriesData.push({
                    name: subCategoryName,
                    id: subCat.id,
                    specificItems: specificItems
                });
            }

            return {
                mainCategory: {
                    name: mainCategoryName,
                    id: mainCategory.id
                },
                subCategories: subCategoriesData,
                createdAt: mainCategory.createdAt,
                updatedAt: mainCategory.updatedAt
            };
        } else {
            const subCategoriesData = [];

            for (const subCat of mainCategory.subCategories) {
                subCategoriesData.push({
                    name: subCat.name,
                    id: subCat.id,
                    specificItems: subCat.specificItems.map(item => ({
                        name: item.name,
                        id: item.id
                    }))
                });
            }

            return {
                mainCategory: {
                    name: mainCategory.name,
                    id: mainCategory.id
                },
                subCategories: subCategoriesData,
                createdAt: mainCategory.createdAt,
                updatedAt: mainCategory.updatedAt
            };
        }
    },

    async updateCategory(id, updateData, lang = "en", reqDetails = {}) {
        const mainCategoryId = parseInt(id, 10);
        if (isNaN(mainCategoryId)) {
            throw new Error("Invalid category ID format.");
        }

        const currentMainCategory = await prisma.mainCategoryOption.findUnique({
            where: { id: mainCategoryId },
            include: {
                subCategories: {
                    include: {
                        specificItems: true
                    }
                }
            }
        });

        if (!currentMainCategory) return null;

        let updatedMainCategory = currentMainCategory;

        if (lang === "ar") {
            // Arabic input - convert to English for DB operations
            if (updateData.mainCategory) {
                const mainCategoryName = await translateText(updateData.mainCategory, "EN-US", "AR");
                
                const existingMainCategory = await prisma.mainCategoryOption.findFirst({
                    where: { 
                        name: mainCategoryName,
                        NOT: { id: mainCategoryId }
                    }
                });
                
                if (!existingMainCategory) {
                    updatedMainCategory = await prisma.mainCategoryOption.update({
                        where: { id: mainCategoryId },
                        data: { name: mainCategoryName }
                    });
                }
            }

            if (updateData.subCategories) {
                const subCategories = Array.isArray(updateData.subCategories) 
                    ? updateData.subCategories 
                    : [updateData.subCategories];

                for (const subCatData of subCategories) {
                    const parts = subCatData.split('-');
                    if (parts.length === 2 && !isNaN(parseInt(parts[1]))) {
                        // Update existing subcategory by ID
                        const subCatId = parseInt(parts[1]);
                        const subCategoryName = await translateText(parts[0], "EN-US", "AR");
                        
                        const existingSubCategory = await prisma.subCategoryOption.findFirst({
                            where: { 
                                name: subCategoryName,
                                NOT: { id: subCatId }
                            }
                        });
                        
                        if (!existingSubCategory) {
                            await prisma.subCategoryOption.update({
                                where: { id: subCatId },
                                data: { name: subCategoryName }
                            });
                        }
                    } else {
                        // Create new subcategory
                        const subCategoryName = await translateText(subCatData, "EN-US", "AR");
                        
                        const existingSubCategory = await prisma.subCategoryOption.findFirst({
                            where: { 
                                name: subCategoryName,
                                mainCategoryId: mainCategoryId
                            }
                        });
                        
                        if (!existingSubCategory) {
                            await prisma.subCategoryOption.create({
                                data: {
                                    name: subCategoryName,
                                    mainCategoryId: mainCategoryId
                                }
                            });
                        }
                    }
                }
            }

            if (updateData.specificItem) {
                const specificItems = Array.isArray(updateData.specificItem) 
                    ? updateData.specificItem 
                    : [updateData.specificItem];

                for (const itemData of specificItems) {
                    const parts = itemData.split('-');
                    if (parts.length === 2 && !isNaN(parseInt(parts[1]))) {
                        // Update existing specific item by ID
                        const itemId = parseInt(parts[1]);
                        const itemName = await translateText(parts[0], "EN-US", "AR");
                        
                        // Check if the item exists before updating
                        const itemToUpdate = await prisma.specificItemOption.findUnique({
                            where: { id: itemId }
                        });
                        
                        if (itemToUpdate) {
                            const existingSpecificItem = await prisma.specificItemOption.findFirst({
                                where: { 
                                    name: itemName,
                                    NOT: { id: itemId }
                                }
                            });
                            
                            if (!existingSpecificItem) {
                                await prisma.specificItemOption.update({
                                    where: { id: itemId },
                                    data: { name: itemName }
                                });
                            }
                        }
                    } else {
                        // Create new specific item
                        const itemName = await translateText(itemData, "EN-US", "AR");
                        
                        const existingSpecificItem = await prisma.specificItemOption.findFirst({
                            where: { 
                                name: itemName,
                                mainCategoryId: mainCategoryId
                            }
                        });
                        
                        if (!existingSpecificItem) {
                            await prisma.specificItemOption.create({
                                data: {
                                    name: itemName,
                                    mainCategoryId: mainCategoryId,
                                    subCategoryId: currentMainCategory.subCategories[0]?.id
                                }
                            });
                        }
                    }
                }
            }

        } else {
            // English input
            if (updateData.mainCategory) {
                const existingMainCategory = await prisma.mainCategoryOption.findFirst({
                    where: { 
                        name: updateData.mainCategory,
                        NOT: { id: mainCategoryId }
                    }
                });
                
                if (!existingMainCategory) {
                    updatedMainCategory = await prisma.mainCategoryOption.update({
                        where: { id: mainCategoryId },
                        data: { name: updateData.mainCategory }
                    });
                }
            }

            if (updateData.subCategories) {
                const subCategories = Array.isArray(updateData.subCategories) 
                    ? updateData.subCategories 
                    : [updateData.subCategories];

                for (const subCatData of subCategories) {
                    const parts = subCatData.split('-');
                    if (parts.length === 2 && !isNaN(parseInt(parts[1]))) {
                        // Update existing subcategory by ID
                        const subCatId = parseInt(parts[1]);
                        
                        const existingSubCategory = await prisma.subCategoryOption.findFirst({
                            where: { 
                                name: parts[0],
                                NOT: { id: subCatId }
                            }
                        });
                        
                        if (!existingSubCategory) {
                            await prisma.subCategoryOption.update({
                                where: { id: subCatId },
                                data: { name: parts[0] }
                            });
                        }
                    } else {
                        // Create new subcategory
                        const existingSubCategory = await prisma.subCategoryOption.findFirst({
                            where: { 
                                name: subCatData,
                                mainCategoryId: mainCategoryId
                            }
                        });
                        
                        if (!existingSubCategory) {
                            await prisma.subCategoryOption.create({
                                data: {
                                    name: subCatData,
                                    mainCategoryId: mainCategoryId
                                }
                            });
                        }
                    }
                }
            }

            if (updateData.specificItem) {
                const specificItems = Array.isArray(updateData.specificItem) 
                    ? updateData.specificItem 
                    : [updateData.specificItem];

                for (const itemData of specificItems) {
                    const parts = itemData.split('-');
                    if (parts.length === 2 && !isNaN(parseInt(parts[1]))) {
                        // Update existing specific item by ID
                        const itemId = parseInt(parts[1]);
                        
                        // Check if the item exists before updating
                        const itemToUpdate = await prisma.specificItemOption.findUnique({
                            where: { id: itemId }
                        });
                        
                        if (itemToUpdate) {
                            const existingSpecificItem = await prisma.specificItemOption.findFirst({
                                where: { 
                                    name: parts[0],
                                    NOT: { id: itemId },
                                    mainCategoryId: itemToUpdate.mainCategoryId
                                }
                            });
                            
                            if (!existingSpecificItem) {
                                await prisma.specificItemOption.update({
                                    where: { id: itemId },
                                    data: { name: parts[0] }
                                });
                            }
                        }
                    } else {
                        // Create new specific item
                        const existingSpecificItem = await prisma.specificItemOption.findFirst({
                            where: { 
                                name: itemData,
                                mainCategoryId: mainCategoryId
                            }
                        });
                        
                        if (!existingSpecificItem) {
                            await prisma.specificItemOption.create({
                                data: {
                                    name: itemData,
                                    mainCategoryId: mainCategoryId,
                                    subCategoryId: currentMainCategory.subCategories[0]?.id
                                }
                            });
                        }
                    }
                }
            }
        }

        // Update Redis cache for all affected subcategories
        try {
            const updatedCategory = await prisma.mainCategoryOption.findUnique({
                where: { id: mainCategoryId },
                include: {
                    subCategories: {
                        include: {
                            specificItems: true
                        }
                    }
                }
            });

            for (const subCat of updatedCategory.subCategories) {
                if (lang === "ar") {
                    // Store original Arabic values in cache
                    const cacheData = {
                        mainCategory: { name: updateData.mainCategory || updatedCategory.name, id: updatedCategory.id },
                        subCategories: { name: subCat.name, id: subCat.id },
                        specificItem: subCat.specificItems.map(item => ({
                            name: item.name,
                            id: item.id
                        })),
                        createdAt: updatedCategory.createdAt,
                        updatedAt: updatedCategory.updatedAt
                    };
                    await redisClient.setEx(
                        cacheKeys.categoryAr(mainCategoryId, subCat.id),
                        AR_CACHE_EXPIRATION,
                        JSON.stringify(cacheData)
                    );
                } else {
                    // Convert to Arabic and store in cache
                    const arMainCategory = await translateText(updatedCategory.name, "AR", "EN");
                    const arSubCategory = await translateText(subCat.name, "AR", "EN");
                    const arSpecificItems = await Promise.all(
                        subCat.specificItems.map(async (item) => ({
                            name: await translateText(item.name, "AR", "EN"),
                            id: item.id
                        }))
                    );

                    const cacheData = {
                        mainCategory: { name: arMainCategory, id: updatedCategory.id },
                        subCategories: { name: arSubCategory, id: subCat.id },
                        specificItem: arSpecificItems,
                        createdAt: updatedCategory.createdAt,
                        updatedAt: updatedCategory.updatedAt
                    };

                    await redisClient.setEx(
                        cacheKeys.categoryAr(mainCategoryId, subCat.id),
                        AR_CACHE_EXPIRATION,
                        JSON.stringify(cacheData)
                    );
                }
            }
        } catch (error) {
            console.error('Redis cache error during update:', error.message);
        }

        // Invalidate all categories cache
        try {
            await redisClient.del(cacheKeys.allCategoriesAr());
        } catch (error) {
            console.error('Redis cache error during cache invalidation:', error.message);
        }

        // Record audit log
        recordAuditLog(AuditLogAction.CATEGORY_UPDATED, {
            userId: reqDetails.actorUserId,
            entityName: 'Category',
            entityId: mainCategoryId.toString(),
            oldValues: currentMainCategory,
            newValues: updateData,
            description: `Category '${updatedMainCategory.name}' updated.`,
            ipAddress: reqDetails.ipAddress,
            userAgent: reqDetails.userAgent,
        });

        // Return updated category in the same format
        return await this.getCategoryById(mainCategoryId, lang);
    },

    async deleteCategory(id, lang = "en", reqDetails = {}) {
        const mainCategoryId = parseInt(id, 10);
        if (isNaN(mainCategoryId)) {
            throw new Error("Invalid category ID format.");
        }

        const categoryToDelete = await prisma.mainCategoryOption.findUnique({
            where: { id: mainCategoryId },
            include: {
                subCategories: {
                    include: {
                        specificItems: true
                    }
                }
            }
        });

        if (!categoryToDelete) return null;

        // Delete from Redis cache first - clear all AR caches related to this category
        try {
            // Delete individual subcategory caches
            for (const subCat of categoryToDelete.subCategories) {
                await redisClient.del(cacheKeys.categoryAr(mainCategoryId, subCat.id));
            }
            // Clear all categories cache in Arabic
            await redisClient.del(cacheKeys.allCategoriesAr());
        } catch (error) {
            console.error('Redis cache error during category deletion:', error.message);
        }

        // Delete specific items first
        for (const subCat of categoryToDelete.subCategories) {
            await prisma.specificItemOption.deleteMany({
                where: { subCategoryId: subCat.id }
            });
        }

        // Delete sub categories
        await prisma.subCategoryOption.deleteMany({
            where: { mainCategoryId: mainCategoryId }
        });

        // Delete main category
        const deletedCategory = await prisma.mainCategoryOption.delete({
            where: { id: mainCategoryId }
        });

        // Record audit log
        recordAuditLog(AuditLogAction.CATEGORY_DELETED, {
            userId: reqDetails.actorUserId,
            entityName: 'Category',
            entityId: categoryToDelete.id.toString(),
            oldValues: categoryToDelete,
            description: `Category '${categoryToDelete.name}' deleted.`,
            ipAddress: reqDetails.ipAddress,
            userAgent: reqDetails.userAgent,
        });

        return deletedCategory;
    },

    async deleteSubCategory(id, reqDetails = {}) {
        const subCategoryId = parseInt(id, 10);
        if (isNaN(subCategoryId)) {
            throw new Error("Invalid subcategory ID format.");
        }

        const subCategoryToDelete = await prisma.subCategoryOption.findUnique({
            where: { id: subCategoryId },
            include: {
                specificItems: true
            }
        });

        if (!subCategoryToDelete) return null;

        // Delete from Redis cache first - clear AR caches
        try {
            // Delete specific subcategory cache
            await redisClient.del(cacheKeys.categoryAr(subCategoryToDelete.mainCategoryId, subCategoryId));
            // Clear all categories cache in Arabic
            await redisClient.del(cacheKeys.allCategoriesAr());
        } catch (error) {
            console.error('Redis cache error during subcategory deletion:', error.message);
        }

        // Delete specific items first
        await prisma.specificItemOption.deleteMany({
            where: { subCategoryId: subCategoryId }
        });

        // Delete sub category
        const deletedSubCategory = await prisma.subCategoryOption.delete({
            where: { id: subCategoryId }
        });

        // Record audit log
        recordAuditLog(AuditLogAction.CATEGORY_DELETED, {
            userId: reqDetails.actorUserId,
            entityName: 'SubCategory',
            entityId: subCategoryToDelete.id.toString(),
            oldValues: subCategoryToDelete,
            description: `Sub-category '${subCategoryToDelete.name}' deleted.`,
            ipAddress: reqDetails.ipAddress,
            userAgent: reqDetails.userAgent,
        });

        return deletedSubCategory;
    },

    async deleteSpecificItem(id, reqDetails = {}) {
        const specificItemId = parseInt(id, 10);
        if (isNaN(specificItemId)) {
            throw new Error("Invalid specific item ID format.");
        }

        const specificItemToDelete = await prisma.specificItemOption.findUnique({
            where: { id: specificItemId }
        });

        if (!specificItemToDelete) return null;

        // Delete from Redis cache first - clear AR caches
        try {
            // Delete cache for the subcategory that contains this item
            await redisClient.del(cacheKeys.categoryAr(specificItemToDelete.mainCategoryId, specificItemToDelete.subCategoryId));
            // Clear all categories cache in Arabic
            await redisClient.del(cacheKeys.allCategoriesAr());
        } catch (error) {
            console.error('Redis cache error during specific item deletion:', error.message);
        }

        // Delete specific item
        const deletedSpecificItem = await prisma.specificItemOption.delete({
            where: { id: specificItemId }
        });

        // Record audit log
        recordAuditLog(AuditLogAction.CATEGORY_DELETED, {
            userId: reqDetails.actorUserId,
            entityName: 'SpecificItem',
            entityId: specificItemToDelete.id.toString(),
            oldValues: specificItemToDelete,
            description: `Specific item '${specificItemToDelete.name}' deleted.`,
            ipAddress: reqDetails.ipAddress,
            userAgent: reqDetails.userAgent,
        });

        return deletedSpecificItem;
    },
};

export default categoryService;

