"""
SMTP Email Sender (AI-12)
==========================
Sends alert emails via Gmail SMTP with precise delivery timestamp
capture for MTTD computation.  Each email includes the test name,
failure details, affected table, severity, and timestamp.

The sender implements retry logic and captures both the send attempt
timestamp and the confirmed delivery timestamp, which are stored in
the observability database by the ObservabilityWriter (AI-12b).

Project : Severity-Based Alerting in ELT Pipelines
Author  : Manasa Vannam
"""

from __future__ import annotations

import logging
import smtplib
from dataclasses import dataclass
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any

from ..core.config import SMTPConfig
from ..core.exceptions import AlertDeliveryError

logger = logging.getLogger(__name__)


@dataclass
class EmailResult:
    """Captures the outcome of an email send attempt."""
    success: bool
    sent_at: datetime | None
    delivered_at: datetime | None
    error: str | None = None
    message_id: str | None = None


class SMTPSender:
    """
    Sends alert emails via Gmail SMTP.

    Usage::

        sender = SMTPSender(config.smtp)
        result = sender.send_alert(
            subject="CRITICAL: not_null failed on transaction_id",
            body="50 rows with NULL transaction_id detected...",
            severity="critical",
            metadata={"test_name": "not_null_transaction_id", ...}
        )
        print(result.delivered_at)  # UTC timestamp for MTTD
    """

    def __init__(self, config: SMTPConfig):
        self._config = config

    def send_alert(
        self,
        subject: str,
        body: str,
        severity: str = "critical",
        metadata: dict[str, Any] | None = None,
        max_retries: int = 2,
    ) -> EmailResult:
        """
        Send an alert email.

        Args:
            subject:     Email subject line.
            body:        Email body text.
            severity:    Alert severity for the email header.
            metadata:    Additional context to include in the email body.
            max_retries: Number of retry attempts on transient failure.

        Returns:
            EmailResult with delivery timestamps.
        """
        sent_at = datetime.now(timezone.utc)

        msg = self._build_message(subject, body, severity, metadata)

        last_error = None
        for attempt in range(1, max_retries + 1):
            try:
                self._deliver(msg)
                delivered_at = datetime.now(timezone.utc)

                logger.info(
                    "Alert email delivered",
                    extra={
                        "subject": subject,
                        "severity": severity,
                        "sent_at": sent_at.isoformat(),
                        "delivered_at": delivered_at.isoformat(),
                        "attempt": attempt,
                    },
                )

                return EmailResult(
                    success=True,
                    sent_at=sent_at,
                    delivered_at=delivered_at,
                    message_id=msg.get("Message-ID"),
                )

            except Exception as exc:
                last_error = str(exc)
                logger.warning(
                    "Email send attempt %d/%d failed: %s",
                    attempt, max_retries, exc,
                )

        logger.error("All email delivery attempts failed: %s", last_error)
        return EmailResult(
            success=False,
            sent_at=sent_at,
            delivered_at=None,
            error=last_error,
        )

    def test_connection(self) -> bool:
        """Test SMTP connectivity without sending an email."""
        try:
            with smtplib.SMTP(self._config.host, self._config.port, timeout=10) as server:
                if self._config.use_tls:
                    server.starttls()
                if self._config.username:
                    server.login(self._config.username, self._config.password)
            logger.info("SMTP connection test passed")
            return True
        except Exception as exc:
            logger.error("SMTP connection test failed: %s", exc)
            return False

    # ── Internal ─────────────────────────────────────────────────────────

    def _build_message(
        self,
        subject: str,
        body: str,
        severity: str,
        metadata: dict[str, Any] | None,
    ) -> MIMEMultipart:
        """Construct the MIME email message."""
        msg = MIMEMultipart()
        msg["From"] = self._config.sender
        msg["To"] = self._config.recipient
        msg["Subject"] = f"[{severity.upper()}] {subject}"
        msg["X-Alert-Severity"] = severity
        msg["X-Alert-Timestamp"] = datetime.now(timezone.utc).isoformat()

        # Build body with metadata
        full_body = f"Severity: {severity.upper()}\n"
        full_body += f"Time: {datetime.now(timezone.utc).isoformat()}\n"
        full_body += f"\n{body}\n"

        if metadata:
            full_body += "\n--- Alert Metadata ---\n"
            for key, value in metadata.items():
                full_body += f"  {key}: {value}\n"

        full_body += "\n---\n"
        full_body += "ELT Pipeline Alerting System\n"
        full_body += "Dissertation: Severity-Based Alerting in ELT Pipelines\n"

        msg.attach(MIMEText(full_body, "plain"))
        return msg

    def _deliver(self, msg: MIMEMultipart) -> None:
        """Send the message via SMTP."""
        if not self._config.username:
            raise AlertDeliveryError("SMTP credentials not configured")

        with smtplib.SMTP(self._config.host, self._config.port, timeout=30) as server:
            if self._config.use_tls:
                server.starttls()
            server.login(self._config.username, self._config.password)
            server.send_message(msg)
