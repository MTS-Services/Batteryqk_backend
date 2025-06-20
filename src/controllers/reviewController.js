import reviewService from '../services/reviewService.js';

const reviewController = {
    // Create a new review
    async createReview(req, res) {
        try {
            const userUid  = req.user?.uid || null; // null for admin reviews
            const lang = req.query.lang || req.headers['accept-language'] || 'en';
            const reqDetails = {
                ipAddress: req.ip,
                userAgent: req.get('User-Agent')
            };

            const review = await reviewService.createReview(req.body, userUid, lang, reqDetails);
            
            res.status(201).json({
                success: true,
                data: review
            });
        } catch (error) {
            res.status(400).json({
                success: false,
                message: error.message
            });
        }
    },

    // Get all reviews with filters
    async getAllReviews(req, res) {
        try {
            const lang = req.query.lang || req.headers['accept-language'] || 'en';
            const filters = req.query;

            const result = await reviewService.getAllReviews(filters, lang);
            
            res.status(200).json({
                success: true,
                data: result
            });
        } catch (error) {
            res.status(500).json({
                success: false,
                message: error.message
            });
        }
    },

    // Get review by ID
    async getReviewById(req, res) {
        try {
            const { id } = req.params;
            const lang = req.query.lang || req.headers['accept-language'] || 'en';

            const review = await reviewService.getReviewById(id, lang);
            
            if (!review) {
                return res.status(404).json({
                    success: false,
                    message: lang === 'ar' ? 'المراجعة غير موجودة' : 'Review not found'
                });
            }

            res.status(200).json({
                success: true,
                data: review
            });
        } catch (error) {
            res.status(500).json({
                success: false,
                message: error.message
            });
        }
    },

    // Get reviews by user UID
    async getReviewsByUserUid(req, res) {
        try {
            const uid = req.user?.uid;
            const lang = req.query.lang || req.headers['accept-language'] || 'en';

            const reviews = await reviewService.getReviewsByUserUid(uid, lang);
            if (!reviews || reviews.length === 0) {
                return res.status(404).json({
                    success: false,
                    message: lang === 'ar' ? 'لا توجد مراجعات لهذا المستخدم' : 'No reviews found for this user'
                });
            }

            res.status(200).json({
                success: true,
                data: reviews
            });
        } catch (error) {
            res.status(500).json({
                success: false,
                message: error.message
            });
        }
    },

    // Update review
    async updateReview(req, res) {
        try {
            const { id } = req.params;
            const userUid = req.user?.userUid || null; // null for admin updates
            const lang = req.query.lang || req.headers['accept-language'] || 'en';
            const reqDetails = {
                ipAddress: req.ip,
                userAgent: req.get('User-Agent'),
                actorUserId: req.user?.userId
            };

            const updatedReview = await reviewService.updateReview(id, req.body, userUid, lang, reqDetails);
            
            res.status(200).json({
                success: true,
                data: updatedReview
            });
        } catch (error) {
            res.status(400).json({
                success: false,
                message: error.message
            });
        }
    },

    // Delete review
    async deleteReview(req, res) {
        try {
            const { id } = req.params;
            const lang = req.query.lang || req.headers['accept-language'] || 'en';
            const reqDetails = {
                ipAddress: req.ip,
                userAgent: req.get('User-Agent'),
                actorUserId: req.user?.userId
            };

            const result = await reviewService.deleteReview(id, lang, reqDetails);
            if (!result) {
                return res.status(404).json({
                    success: false,
                    message: req.query.lang === 'ar' ? 'المراجعة غير موجودة' : 'Review not found'
                });
            }
            
            res.status(200).json({
                success: true,
                data: result
            });
        } catch (error) {
            res.status(400).json({
                success: false,
                message: error.message
            });
        }
    }
};

export default reviewController;