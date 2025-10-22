import { test, expect } from '@playwright/test';

test.describe('EZPass Dashboard', () => {
    test('loads dashboard view by default', async ({ page }) => {
        await page.goto('http://localhost:3000');
        await expect(page.locator('h1')).toHaveText(/EZ Pass Fraud Detection/i);
        await expect(page.locator('text=Total Alerts (YTD)')).toBeVisible();
    });

    test('can switch to Raw Data view', async ({ page }) => {
        await page.goto('http://localhost:3000');
        await page.click('text=📋 Raw Data');
        await expect(page.locator('input[placeholder="Search transactions..."]')).toBeVisible();
    });

    test('can filter transactions', async ({ page }) => {
        await page.goto('http://localhost:3000');
        await page.click('text=📋 Raw Data');
        await page.click('button:text("Flagged")');
        const rows = await page.locator('tbody tr').count();
        expect(rows).toBeGreaterThan(0);
    });

    test('changes transaction status', async ({ page }) => {
        await page.goto('http://localhost:3000');
        await page.click('text=📋 Raw Data');

        // Find the first status dropdown
        const dropdown = page.locator('tbody tr select').first();
        await dropdown.selectOption('Resolved');

        // Verify the dropdown value changed
        await expect(dropdown).toHaveValue('Resolved');
    });
});
