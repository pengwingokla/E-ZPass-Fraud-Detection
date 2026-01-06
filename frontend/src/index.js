import React from 'react';
import ReactDOM from 'react-dom/client';
import './index.css';

import App from './App';
import { MsalProvider } from "@azure/msal-react";
import { PublicClientApplication } from "@azure/msal-browser";
import { msalConfig } from "./authConfig";

// Create and initialize MSAL instance
const pca = new PublicClientApplication(msalConfig);

// Initialize MSAL
pca.initialize()
  .then(() => {
    console.log("MSAL initialized successfully");
    
    // Handle redirect response if present (this happens after redirect from Microsoft login)
    pca.handleRedirectPromise()
      .then((response) => {
        if (response) {
          console.log("MSAL redirect response received:", response);
        }
      })
      .catch((error) => {
        console.error("MSAL redirect error:", error);
      });

    // Render the app
    const root = ReactDOM.createRoot(document.getElementById('root'));
    root.render(
      <React.StrictMode>
        <MsalProvider instance={pca}>
          <App />
        </MsalProvider>
      </React.StrictMode>
    );
  })
  .catch((error) => {
    console.error("MSAL initialization error:", error);
    // Still render the app even if initialization fails (for debugging)
    const root = ReactDOM.createRoot(document.getElementById('root'));
    root.render(
      <React.StrictMode>
        <MsalProvider instance={pca}>
          <App />
        </MsalProvider>
      </React.StrictMode>
    );
  });
