create table inbox_events (
    event_id        varchar(64)  primary key,
    status          varchar(20)  not null,
    payload         text         not null,
    lease_token     uuid,
    owner           varchar(100),
    locked_until    timestamptz,
    attempts        int          not null default 0,
    next_attempt_at timestamptz  not null,
    reason          varchar(100),
    detail          text,
    core_reference  varchar(100),
    resolution_note text,
    created_at      timestamptz  not null,
    updated_at      timestamptz  not null,
    constraint inbox_events_status_check
        check (status in ('RECEIVED', 'IN_FLIGHT', 'DONE', 'FAILED', 'UNKNOWN'))
);

create index idx_inbox_events_status_lock on inbox_events (status, locked_until);
create index idx_inbox_events_status_next on inbox_events (status, next_attempt_at);
create index idx_inbox_events_status_updated on inbox_events (status, updated_at);

create table inbox_transitions (
    id          bigserial    primary key,
    event_id    varchar(64)  not null references inbox_events (event_id) on delete cascade,
    from_status varchar(20),
    to_status   varchar(20)  not null,
    reason      varchar(100),
    owner       varchar(100),
    attempts    int          not null,
    at          timestamptz  not null default now()
);

create index idx_inbox_transitions_event on inbox_transitions (event_id, id);

create function inbox_record_transition() returns trigger as $$
begin
    if tg_op = 'INSERT' then
        insert into inbox_transitions (event_id, from_status, to_status, reason, owner, attempts)
        values (new.event_id, null, new.status, new.reason, new.owner, new.attempts);
    elsif old.status is distinct from new.status then
        insert into inbox_transitions (event_id, from_status, to_status, reason, owner, attempts)
        values (new.event_id, old.status, new.status, new.reason, new.owner, new.attempts);
    end if;
    return new;
end;
$$ language plpgsql;

create trigger trg_inbox_transition
    after insert or update of status on inbox_events
    for each row execute function inbox_record_transition();
