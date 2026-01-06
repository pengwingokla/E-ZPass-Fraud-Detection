// src/authConfig.js
const redirectUri = process.env.REACT_APP_REDIRECT_URL || window.location.origin;

export const msalConfig = {
  auth: {
    clientId: "12af513b-3ca4-437a-ae71-ce0f6fb2fd1b",
    authority: "https://login.microsoftonline.com/ae36bb0f-48db-4843-a540-e6473f2b5f05",
    redirectUri: redirectUri,
  },
  cache: {
    cacheLocation: "sessionStorage", // This configures where your cache will be stored
    storeAuthStateInCookie: false, // Set this to "true" if you are having issues on IE11 or Edge
  },
};

export const loginRequest = {
  scopes: ["User.Read"],
};
