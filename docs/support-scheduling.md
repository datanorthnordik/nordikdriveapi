# Support scheduling

The `internal/supportschedule` package provides survivor-facing support-call
booking and a fair on-call rota. It is registered by `cmd/server/main.go` and
uses the application's existing PostgreSQL database, authentication cookie,
and mail service.

## Defaults and configuration

- Service window: Monday to Friday, **08:30–16:30 America/Toronto**.
- Initial horizon: the current day plus the next thirteen calendar days. This
  yields the next two weeks of weekdays; the recurring job advances the last
  day as time moves forward.
- Enabled durations: 30 and 60 minutes. A Manager can update this list to
  15/30/45/60 (or any 15-minute increment from 15 to 240) through the settings
  API. The default duration must be one of the enabled values.
- A staff member accrues fairness time only when a scheduled call is marked
  `completed`; the actual start/end entries determine the recorded minutes.

The reviewed PostgreSQL DDL and idempotent `Manager` role seed are in
`db-init/init.sql`; this feature does **not** use GORM AutoMigrate. Apply that
SQL as part of the normal database-initialization/deployment process. Promote
the one or two authorized people by changing their existing `users.role` to
`Manager`. Administrators retain all manager capabilities.

## Workflow

1. Managers add active support-team members using `PUT /api/support-schedule/team/:userID`.
2. The scheduler assigns one active member to every weekday. It selects the
   eligible person with the fewest completed support minutes, then uses least
   recently assigned and user ID as deterministic tie-breakers.
3. A normal booking uses the on-call person and immediately sends that person
   the existing application email notification. A named-person booking holds
   the selected person's slot in `awaiting_staff_approval` until that person
   approves or declines it in **My Support**.
4. Staff enter actual start and end times when they mark a call done. The
   calculated duration updates the fairness total.
5. A staff member can record their own absence. Managers can record an absence
   for another staff member or the whole team. Partial absences remove only
   overlapping slots. Full workday coverage immediately triggers a fair daily
   reassignment. Calls that now overlap an absence are marked
   `needs_reschedule` for manager action.

The support maintenance runner executes at application startup and every five
minutes. It maintains the two-week rolling horizon and rechecks assignments.
Managers can also call the maintenance API for an immediate check.

## API surface

All routes are authenticated and begin with `/api/support-schedule`.

| Route | Purpose |
| --- | --- |
| `GET /settings`, `PUT /settings` | Read settings; Manager/Admin updates settings. |
| `GET /availability?date=YYYY-MM-DD&duration_minutes=30&staff_id=` | Bookable slots and the relevant staff member. `staff_id` is optional for a named-person request. |
| `GET /schedule` | Current two-week rota, including the assigned person for each weekday. |
| `GET /team` | Active staff choices without email addresses. |
| `GET /team/manage`, `PUT /team/:userID` | Manager team administration. |
| `GET/POST /calls` | A caller's/assignee's calls and new booking requests. Managers can use `?scope=manage`. |
| `PUT /calls/:id/approval` | The requested staff member approves or declines an off-rota request. |
| `PUT /calls/:id/complete` | Assignee or Manager records actual start/end and completion. |
| `PUT /calls/:id/reassign`, `PUT /schedule/:date/reassign` | Manager/Admin call or rota reassignment with a reason. |
| `GET/POST /unavailability` | Personal/team absence management. |
| `POST /maintenance` | Manager/Admin immediate rolling-horizon and rebalance check. |

## Deferred integrations

`MeetingProvider` and `HRAvailabilityProvider` are deliberately explicit
interfaces in `providers.go`:

- Zoom is not called yet. New records are persisted as
  `meeting_provider=zoom`, `meeting_status=pending_provider`, which is the
  safe placeholder for a future OAuth/API implementation.
- SageHR is not called yet. HR-driven absence synchronization can implement
  `HRAvailabilityProvider` and create the same local availability records used
  by the application.

No unresolved remote URLs or credentials were introduced for either provider.

## Verification

Run from `nordikdriveapi`:

```powershell
$env:GOCACHE = 'C:\Nordik Projects\nordikdriveapi\.gocache'
go test ./internal/supportschedule
go test ./cmd/server
```

The package test suite verifies the two-week weekday rota, a full-day
unavailability reassignment, named-person approval, actual-minute recording,
and fairness ordering. Run `npm run build` from `nordik-drive` to verify the
Contact Us dialog, My Support page, manager controls, and routing compile.
