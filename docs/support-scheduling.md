# Support-call scheduling

The support-call feature is a role-separated scheduling workflow. Its backend
is authoritative for availability, approval, fairness, concurrency and audit
history; a client cannot create a daily assignment or book a conflicting slot.

## Deployment and schema

Fresh installations use the support-call tables in `db-init/init.sql`:

- `support_assignments` — one primary assignee for every calendar date.
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

## Verification

```powershell
$env:GOCACHE = 'C:\Nordik Projects\nordikdriveapi\.gocache'
go test ./internal/supportschedule
go test ./internal/jobs ./cmd/server
```

The support-schedule suite covers the 14-day idempotent horizon, full-day
fair reassignment, partial availability, overlap prevention, specific-person
approval, actual-duration fairness, audit entries, and role separation.
