
---

### File 2: `docs/TROUBLESHOOTING.md`

```markdown
# 🔧 Troubleshooting Guide

Common issues and their solutions.

## Workflow Fails Immediately

**Symptom:** Workflow fails within seconds

**Causes:**
- Missing GitHub Secrets
- Incorrect secret names
- Python dependencies failed to install

**Solution:**
1. Check Settings → Secrets → Actions
2. Verify all 4 secrets exist with correct names
3. Check workflow logs for specific error

## Authentication Failed

**Symptom:** "Drive auth failed" in logs

**Causes:**
- Invalid refresh token
- Expired OAuth credentials
- Wrong client ID/secret

**Solution:**
1. Re-run `get_token.py` to generate new refresh token
2. Update `GOOGLE_REFRESH_TOKEN` secret
3. Verify OAuth credentials in Google Cloud Console

## Upload Fails with 403

**Symptom:** "Service Accounts do not have storage quota"

**Cause:** Using Service Account instead of OAuth

**Solution:**
1. Ensure using OAuth Refresh Token method
2. Delete any `GOOGLE_DRIVE_CREDENTIALS` secret
3. Use `GOOGLE_REFRESH_TOKEN`, `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET`

## Files Not Appearing in Drive

**Symptom:** Download succeeds, upload fails silently

**Causes:**
- Folder ID incorrect
- Service account lacks folder access

**Solution:**
1. Verify folder ID matches your Drive folder
2. Ensure OAuth token has Drive scope
3. Check upload logs for specific errors

## 404 Errors for Recent Months

**Symptom:** "Not found after 5 attempts" for recent months

**Cause:** DGI hasn't published that month yet

**Solution:** This is normal! Script will catch it when published.

## GitHub Actions Rate Limited

**Symptom:** "API rate limit exceeded"

**Cause:** Too many workflow runs

**Solution:**
1. Wait for rate limit to reset (usually 1 hour)
2. Reduce manual trigger frequency
3. Daily schedule is within limits