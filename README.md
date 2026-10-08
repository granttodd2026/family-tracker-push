# family-tracker-push

Reminder sender for the family tracker PWA. Runs on GitHub Actions every 5 minutes.

It never receives a family key. Phones post, over public Nostr relays and encrypted only to this sender's key,
the next due times (nursing, meds), generic reminder labels, and their Web Push subscriptions. The sender groups
a family by a public key derived one-way from the family code, sends Web Push (VAPID, payload end-to-end encrypted),
and remembers what it has sent in an event encrypted to itself.

Secrets: SERVER_SK, VAPID_PUBLIC, VAPID_PRIVATE.
