// APEX PM2 ecosystem configuration — ADDITIVE.
//
// This file registers the `lore_tracker` PM2 process. It does NOT
// replace or modify the existing `whale_sniper` PM2 entry, which was
// started ad-hoc and is expected to remain running independently.
//
// Usage:
//   pm2 start apex-ecosystem.config.js          # register + start lore_tracker
//   pm2 restart lore_tracker                    # restart after code changes
//   pm2 logs lore_tracker --lines 100           # tail PM2 stdout/stderr
//   pm2 save                                    # persist process list across reboots
//
// Logs:
//   ~/apex-bot/logs/lore_tracker.log  — application log, rotated daily, 7-day history
//   ~/.pm2/logs/lore_tracker-out.log  — PM2-captured stdout
//   ~/.pm2/logs/lore_tracker-error.log — PM2-captured stderr
//
// Running --send-top3-now / --send-lore-now manually does NOT go
// through PM2 — invoke directly:
//   cd ~/apex-bot && venv/bin/python lore_tracker.py --send-top3-now
//   cd ~/apex-bot && venv/bin/python lore_tracker.py --send-lore-now

module.exports = {
  apps: [
    {
      name:               "lore_tracker",
      script:             "lore_tracker.py",
      interpreter:        "/home/polybot/apex-bot/venv/bin/python",
      cwd:                "/home/polybot/apex-bot",
      // Crash policy — restart on unhandled exception, but stop
      // trying if it's flapping (10 restarts within 60s of bot
      // never reaching uptime >= 30s).
      autorestart:        true,
      restart_delay:      5000,
      max_restarts:       10,
      min_uptime:         30000,
      // RSS ceiling — restart if memory usage grows past 200MB.
      // Polling loop holds open_positions / closed_trades in memory;
      // worst case after 30 days is well under 50MB so 200MB is a
      // generous cap that still catches a runaway leak.
      max_memory_restart: "200M",
      env: {
        // Empty — all real config comes from ~/apex-bot/.env via
        // python-dotenv at startup. NEVER set TELEGRAM_BOT_TOKEN /
        // CLAUDE_API_KEY / SOLANA_RPC / HELIUS_API_KEY /
        // TELEGRAM_BOT_TOKEN_LORE here; PM2 ecosystem files are
        // committed to git.
      },
    },
  ],
};
