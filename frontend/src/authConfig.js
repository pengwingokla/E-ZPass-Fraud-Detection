// src/authConfig.js
export const msalConfig = {
  auth: {
    clientId: "12af513b-3ca4-437a-ae71-ce0f6fb2fd1b",
    authority: "https://login.microsoftonline.com/ae36bb0f-48db-4843-a540-e6473f2b5f05",
    redirectUri: "https://gray-ground-0a1e30d0f.3.azurestaticapps.net",
  },
};

export const loginRequest = {
  scopes: ["User.Read"],
};
