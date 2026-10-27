# Savour Vih: Zombie Defense

Co-op 3D zombie tower defense. This folder contains the game and its own multiplayer server, ready for Render.

## Files
- `server.js` - serves the game and relays players between browsers (WebSockets)
- `package.json` - tells Render to install `ws` and start the server
- `public/index.html` - the game

## Deploy on Render
1. Put these files in a GitHub repository (keep the `public` folder).
2. On Render: **New > Web Service**, connect GitHub, pick the repository.
3. Settings:
   - Runtime: **Node**
   - Build command: `npm install`
   - Start command: `node server.js`
   - Instance type: **Free** (or paid to avoid sleeping)
4. Click **Create Web Service**. When it says "Live", open the `.onrender.com` link.

Everyone who opens that link joins the same survivor camp.

## Notes
- Free Render services sleep after a while with no visitors; the first visit can take up to a minute to wake it.
- Coins, sentries and gear are saved in each player's browser.
- Health check URL: `/health`
