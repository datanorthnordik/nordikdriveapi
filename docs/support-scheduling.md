# Support-call scheduling

The support-call feature is a role-separated scheduling workflow. Its backend
is authoritative for availability, approval, fairness, concurrency and audit
history; a client cannot create a daily assignment or book a conflicting slot.

## Deployment and schema

Fresh installations use the support-call tables in `db-init/init.sql`:

- `support_assignments` — one primary assignee for each Monday–Friday support
  date. Weekend rows remain uncovered so the 14-calendar-day horizon is still
  visible, but Saturday and Sunday have no support coverage or bookable slots.
- `support_staff_availabilities` — full-day and multiple partial-day records.
- `support_call_requests` — requester intent, approval and alternatives.
- `support_calls` — the reserved/scheduled call and actual completed time.
- `support_schedule_audit_logs` — assignment, availability, approval and
  actual-duration history.

Existing deployments of the superseded implementation must export any data
they need, run `db-init/remove-legacy-support-scheduling.sql` once, then apply
`db-init/init.sql`. The cleanup removes only the old denormalized scheduling
tables (`support_daily_assignments`, `support_staff_unavailabilities`, and the
old `support_call_requests`); the unrelated general `support_requests` table,
settings, and support-team membership remain.

## Roles

- Regular users can create and view their own support-call requests and calls.
- Support admins are active `support_team_members` whose user role is `Admin`.
  They manage only their own availability in their Profile, review requests
  assigned to them, and record actual call times.
- Managers administer the support-admin membership, view the complete rota,
  availability, requests and calls, and can reassign an existing daily primary
  assignee or an individual call. Managers cannot create normal daily slots.

Availability endpoints live only below `/api/support-schedule/profile` so the
frontend can keep availability controls in the admin Profile rather than a
header, Requests, or Support Calls page.

## Scheduler and fairness

At startup, `RunScheduledMaintenance` creates one assignment for today and
the following thirteen calendar days. The daily midnight job invokes the same
idempotent operation; it never duplicates an assignment and fills only a
missing date in the rolling horizon.

Automatic selection is deterministic:

1. Lowest completed `actual_duration_minutes`.
2. Fewest primary assignment days.
3. Oldest preceding assignment date.
4. Lowest user ID.

An active support admin marked fully unavailable cannot be selected. If the
current primary assignee becomes fully unavailable, the service replaces them
using those same rules and moves compatible normal requests. Direct requests
or calls that cannot safely move are marked `alternative_time_proposed`.

## API

All routes are authenticated and start with `/api/support-schedule`.

| Route | Purpose |
| --- | --- |
| `GET /availability` | Available slots for the date's primary assignee, or a selected support admin. |
| `GET /calendar` | Booking-horizon calendar with the assigned support person, day state, available-slot count, call count, and role-appropriate day details. |
| `GET/POST /requests` | User requests; `scope=staff` for the active admin queue and `scope=manage` for managers. |
| `PUT /requests/:id/decision` | Assigned support admin approves, rejects, or proposes an alternative. |
| `PUT /requests/:id/accept-alternative`, `PUT /requests/:id/cancel` | Requester actions. |
| `GET /calls` | Calls for the caller/assignee; staff and manager scopes are role checked. |
| `PUT /calls/:id/complete` | Assigned support admin or manager records actual start/end and notes. |
| `PUT /calls/:id/reassign`, `GET /schedule`, `PUT /schedule/:date/reassign` | Manager-only conflict and rota controls. |
| `GET /fairness`, `GET /audit-log` | Manager-only actual-hours, assignment-history, and audit views. |
| `GET /profile` | Support admin's assignments, approved calls, direct requests, and availability. |
| `GET/POST /profile/availability`, `PUT/DELETE /profile/availability/:id` | Support admin's own availability only. |

The PostgreSQL exclusion constraint on `support_calls` prevents overlapping
active reservations even when two bookings race. The service also uses
transactions and locks around booking, approval, reassignment and completion.

## Zoom meetings

Zoom is an optional backend-only integration. Call duration and scheduling time
zone continue to come from `support_schedule_settings`; they are not duplicated
in Cloud Run environment variables.

Each active support admin must be an active user in the manager's Zoom account,
using the exact same email address as their NORDIK Drive user. Their Zoom
Workplace license can remain **Unassigned**, but **Zoom Meetings Basic** must be
selected. The assigned support admin owns and hosts the meeting. The requester,
external customers, and managers use the participant link and the manager does
not need to start or attend the meeting.

### One-time database migration

Run `db-init/add-support-call-zoom.sql` against an existing database before
enabling Zoom. Fresh databases get the same columns from `db-init/init.sql`.
The migration is additive and preserves existing support-call data.

### Create the Zoom application and obtain values

1. Sign in to <https://marketplace.zoom.us/> as the Zoom account owner/admin.
2. Select **Develop**, **Build App**, then **Server-to-Server OAuth App**.
3. Add and activate these account-level granular scopes:
   - `meeting:write:meeting:admin`
   - `meeting:read:meeting:admin`
   - `meeting:update:meeting:admin`
   - `meeting:delete:meeting:admin`
4. Open the app's **App Credentials** page. Copy the **Account ID**, **Client
   ID**, and **Client Secret** into Google Secret Manager. Never place them in
   source control, frontend variables, SQL, logs, or email.

### Cloud Run environment

The complete set of new values is:

| Cloud Run variable | Required value | Where it comes from |
| --- | --- | --- |
| `ZOOM_ENABLED` | `true` after the migration and secrets are ready; otherwise `false`/unset | Chosen deployment flag |
| `ZOOM_ACCOUNT_ID` | Zoom Account ID | Server-to-Server OAuth app, **App Credentials** |
| `ZOOM_CLIENT_ID` | Zoom Client ID | Server-to-Server OAuth app, **App Credentials** |
| `ZOOM_CLIENT_SECRET` | Zoom Client Secret | Server-to-Server OAuth app, **App Credentials** |

Keep the three credentials as Secret Manager-backed environment variables. A
typical deployment update is:

```powershell
gcloud run services update SERVICE_NAME `
  --region REGION `
  --set-env-vars ZOOM_ENABLED=true `
  --set-secrets ZOOM_ACCOUNT_ID=nordik-zoom-account-id:latest,ZOOM_CLIENT_ID=nordik-zoom-client-id:latest,ZOOM_CLIENT_SECRET=nordik-zoom-client-secret:latest
```

The Cloud Run service account needs Secret Manager Secret Accessor permission
for those three secrets. The API fails fast at startup when `ZOOM_ENABLED=true`
and any credential is missing; with Zoom disabled, ordinary support scheduling
continues without Zoom controls.

### Lifecycle and security

- A meeting is created only after a request becomes approved.
- The meeting is updated after an accepted time change.
- Reassignment deletes the former host's meeting and creates a separate meeting
  under the replacement support admin's Zoom email.
- Rejection, cancellation, or a required alternative time deletes the existing
  Zoom meeting.
- Startup/daily maintenance retries failed or interrupted synchronization and
  creates meetings for pre-existing approved calls after Zoom is first enabled.
- Participant links are returned only through authenticated support-call APIs
  and included in role-specific notification emails.
- Host `start_url` values are never stored or emailed. The assigned support
  admin alone can request a fresh link through the authenticated Start Zoom
  meeting action; requesters and managers cannot access it.
- Meetings use a cryptographically generated per-call passcode, waiting room,
  no Personal Meeting ID, no
  join-before-host, no required Zoom sign-in for external customers, muted
  entry, and no automatic recording.

## Verification

```powershell
$env:GOCACHE = 'C:\Nordik Projects\nordikdriveapi\.gocache'
go test ./internal/supportschedule
go test ./internal/jobs ./cmd/server
```

The support-schedule suite covers the 14-day idempotent horizon, full-day
fair reassignment, partial availability, overlap prevention, specific-person
approval, actual-duration fairness, audit entries, and role separation.
