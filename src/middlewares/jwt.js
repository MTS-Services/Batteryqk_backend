import jwt from 'jsonwebtoken';


const generateToken = (uid) => {
  return jwt.sign({ uid }, process.env.SECRET_CODE, {

   
  });
};

export { generateToken };