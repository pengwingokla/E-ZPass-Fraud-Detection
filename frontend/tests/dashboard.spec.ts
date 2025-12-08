import { test, expect } from '@playwright/test';

test.describe('EZPass Login', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('http://localhost:3000');
  });

  test('login button is visible', async ({ page }) => {
    const loginButton = page.locator('button:has-text("Sign in with Microsoft")');
    await expect(loginButton).toBeVisible();
  });

  test('login button has correct text and icon', async ({ page }) => {
    const loginButton = page.locator('button:has-text("Sign in with Microsoft")');
    await expect(loginButton.locator('span')).toHaveText('Sign in with Microsoft');
    await expect(loginButton.locator('svg')).toBeVisible();
  });

});
