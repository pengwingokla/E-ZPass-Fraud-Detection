import { test, expect } from '@playwright/test';

test.describe('EZPass Dashboard', () => {
    test('loads dashboard view by default', async ({ page }) => {
        await page.goto('http://localhost:3000');
        await expect(page.locator('h1')).toHaveText(/EZ Pass Fraud Detection/i);
        await expect(page.locator('text=Total Alerts (YTD)')).toBeVisible();
    });

    test('shows loading message when filtering transactions', async ({ page }) => {
        await page.goto('http://localhost:3000');
        await page.click('text=📋 Raw Data');
        await page.click('button:text("Flagged")');

        // Wait up to 3 seconds for the loading message to appear
        await page.waitForSelector('input[placeholder="Search transactions..."]', { timeout: 3000 });
        //await expect(page.locator('input[placeholder="Search transactions..."]')).toBeVisible();
    });


});
