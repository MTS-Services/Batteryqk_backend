import userService from '../services/userService.js';
import { getLanguage, translate } from '../utils/i18n.js';
import { recordAuditLog } from '../utils/auditLogHandler.js'; // Assuming you have an audit log utility
import { AuditLogAction } from '@prisma/client';             // Added
import { generateToken } from '../middlewares/jwt.js'; // For login, if implemented

const userController = {
  async createUser(req, res, next) {
    const lang = getLanguage(req);
    console.log("Create user request received", lang); // Debugging log
    try {

    
      // In a real app, validate req.body first (e.g., using Joi or express-validator)
      const { fname, lname, email, password, uid } = req.body;
      if (!fname || !lname || !email || !password || !uid) {
        return res.status(400).json({ message: translate ? translate('all_fields_required', lang) : 'All fields are required: fname, lname, email, password, uid' });
      }

      const reqDetails = {
          ipAddress: req.ip,
          userAgent: req.headers['user-agent'],
      };
      const newUser = await userService.createUser({ fname, lname, email,password,uid }, lang, reqDetails);
      res.status(201).json({
          message: translate('user_created', lang, { name: newUser.fname || newUser.email }),
          data: newUser 
      });
    } catch (error) {
      if (error.code === 'P2002' && error.meta?.target?.includes('email')) { // Prisma unique constraint error for email
        console.error(translate('email_required', lang), error);
        return res.status(409).json({ message: translate('email_already_exists', lang) }); // Add to locales
      }
      next(error);
    }
  },

  async getAllUsers(req, res, next) {
    try {
      const lang = getLanguage(req);
      console.log("Get all users request received", lang); // Debugging log
      const users = await userService.getAllUsers(lang);
      res.status(200).json(users);
    } catch (error) {
      next(error);
    }
  },

  async getUserById(req, res, next) {
    const lang = getLanguage(req);
    try {
      const user = await userService.getUserById(req.params.id, lang);
      if (!user) {
        return res.status(404).json({ message: translate('user_not_found', lang) });
      }
      res.status(200).json(user);
    } catch (error) {
      next(error);
    }
  },


  async getUserByUid(req, res, next) {
    const lang = getLanguage(req);
    try {
      const  uid = req.user.uid;
      const user = await userService.getUserByUid(uid, lang);
      if (!user) {
        return res.status(404).json({ message: translate('user_not_found', lang) });
      }
      res.status(200).json(user);
    } catch (error) {
      next(error);
    }
  },

  async updateUser(req, res, next) {
    const lang = getLanguage(req);
    try {
      const reqDetails = {
          ipAddress: req.ip,
          userAgent: req.headers['user-agent'],
          actorUserId: req.user?.id // Assuming verifyToken middleware adds req.user
      };
      // Only allow certain fields to be updated
      const { fname, lname, email } = req.body;
      const updateData = {};
      if (fname !== undefined) updateData.fname = fname;
      if (lname !== undefined) updateData.lname = lname;
      if (email !== undefined) updateData.email = email;


      const updatedUser = await userService.updateUser(req.params.id, updateData, lang, reqDetails);
      if (!updatedUser) {
        return res.status(404).json({ message: translate('user_not_found', lang) });
      }
      res.status(200).json({ 
          message: translate('user_updated', lang, { name: updatedUser.fname || updatedUser.email }),
          data: updatedUser
      });
    } catch (error) {
      if (error.code === 'P2002' && error.meta?.target?.includes('email')) {
        return res.status(409).json({ message: translate('email_already_exists', lang) });
      }
      next(error);
    }
  },

  async deleteUser(req, res, next) {
    const lang = getLanguage(req);
    try {
      const reqDetails = {
          ipAddress: req.ip,
          userAgent: req.headers['user-agent'],
          actorUserId: req.user?.id // Assuming verifyToken middleware adds req.user (e.g. admin)
      };
      const deletedUser = await userService.deleteUser(req.params.id, lang, reqDetails);
      if (!deletedUser) {
        return res.status(404).json({ message: translate('user_not_found', lang) });
      }
      res.status(200).json({ message: translate('user_deleted', lang) });
    } catch (error) {
      next(error);
    }
  },

  
  async loginUser(req, res, next) {
    console.log(req)
    const lang = getLanguage(req);
    console.log("Login request received",lang) ; // Debugging log
    try {
        
      

        //if arabic, teanslate to  english req.body
        if (lang === 'ar') {
            req.body.email = translate(req.body.email, 'en');
            console.log("Translated email:", req.body.email); // Debugging log
            req.body.password = translate(req.body.password, 'en');
        }

        const { email, password } = req.body;
        console.log("Login attempt for email:", email); // Debugging log
        if (!email || !password) {
            return res.status(400).json({ message: translate('email_and_password_required', lang) }); // Add to locales
        }
        const user = await userService.validateUserPassword(email, password); // You'd need to implement this
        if (!user) {
            return res.status(401).json({ message: translate('login_failed_invalid_credentials', lang) });
        }
        const token = generateToken(user.uid);

        recordAuditLog(AuditLogAction.USER_LOGIN, {
            userId: user.id,
            entityName: 'User',
            entityId: user.id,
            description: `User ${user.email} logged in.`,
            ipAddress: req.ip,
            userAgent: req.headers['user-agent'],
        });

        res.status(200).json({
            message: translate('login_successful', lang),
            token,
            user: { id: user.id, uid: user.uid, email: user.email, fname: user.fname }
        });
    } catch (error) {
        next(error);
    }
  },

  async resetPassword(req, res, next) {
    const lang = getLanguage(req);
    try {
      const { email, newPassword } = req.body;
      
      if (!email || !newPassword) {
        return res.status(400).json({ 
          message: translate('email_and_password_required', lang) 
        });
      }

      const reqDetails = {
        ipAddress: req.ip,
        userAgent: req.headers['user-agent'],
        actorUserId: req.user?.id
      };

      const updatedUser = await userService.resetPassword(email, newPassword, lang, reqDetails);
      
      res.status(200).json({
        message: translate('password_reset_successful', lang),
        data: { email: updatedUser.email, name: updatedUser.fname }
      });
    } catch (error) {
      next(error);
    }
  },

  async findUserByEmail(req, res, next) {
    const lang = getLanguage(req);
    try {
      const { email } = req.params;
      
      const user = await userService.findUserByEmail(email, lang);
      
      res.status(200).json({
        message: translate('user_found', lang),
        data: { 
          id: user.id, 
          email: user.email, 
          fname: user.fname, 
          lname: user.lname, 
          uid: user.uid 
        }
      });
    } catch (error) {
      next(error);
    }
  }

};

export default userController;