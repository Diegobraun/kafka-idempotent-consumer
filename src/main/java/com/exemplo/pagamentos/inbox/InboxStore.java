package com.exemplo.pagamentos.inbox;

import com.exemplo.pagamentos.domain.PagamentoEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Repository
public class InboxStore {

    private final JdbcClient jdbc;
    private final ObjectMapper mapper;

    public InboxStore(JdbcClient jdbc, ObjectMapper mapper) {
        this.jdbc = jdbc;
        this.mapper = mapper;
    }

    @Transactional
    public ClaimResult claim(PagamentoEvent event, String owner, double leaseSeconds) {
        Optional<UUID> inserted = jdbc.sql("""
                        insert into inbox_events
                            (event_id, status, payload, lease_token, owner, locked_until,
                             attempts, next_attempt_at, created_at, updated_at)
                        values
                            (:id, 'RECEIVED', :payload, gen_random_uuid(), :owner,
                             now() + make_interval(secs => cast(:lease as double precision)),
                             0, now(), now(), now())
                        on conflict (event_id) do nothing
                        returning lease_token
                        """)
                .param("id", event.eventId())
                .param("payload", toJson(event))
                .param("owner", owner)
                .param("lease", leaseSeconds)
                .query((rs, n) -> rs.getObject("lease_token", UUID.class))
                .optional();

        if (inserted.isPresent()) {
            return new ClaimResult(ClaimDecision.CLAIMED_NEW, new Claim(event.eventId(), inserted.get(), event));
        }

        record Current(InboxStatus status, boolean leaseValid) {}
        Current current = jdbc.sql("""
                        select status, coalesce(locked_until > now(), false) as lease_valid
                        from inbox_events
                        where event_id = :id
                        for update
                        """)
                .param("id", event.eventId())
                .query((rs, n) -> new Current(InboxStatus.valueOf(rs.getString("status")), rs.getBoolean("lease_valid")))
                .single();

        ClaimDecision decision = ClaimPolicy.decide(current.status(), current.leaseValid());
        return switch (decision) {
            case RECLAIMED -> {
                UUID token = jdbc.sql("""
                                update inbox_events
                                set lease_token = gen_random_uuid(),
                                    owner = :owner,
                                    locked_until = now() + make_interval(secs => cast(:lease as double precision)),
                                    updated_at = now()
                                where event_id = :id
                                returning lease_token
                                """)
                        .param("id", event.eventId())
                        .param("owner", owner)
                        .param("lease", leaseSeconds)
                        .query((rs, n) -> rs.getObject("lease_token", UUID.class))
                        .single();
                yield new ClaimResult(decision, new Claim(event.eventId(), token, event));
            }
            case PARKED_IN_FLIGHT -> {
                jdbc.sql("""
                                update inbox_events
                                set status = 'UNKNOWN',
                                    reason = 'REDELIVERED_IN_FLIGHT',
                                    detail = 'Mensagem reentregue com chamada ao core iniciada e lease expirada',
                                    lease_token = null,
                                    locked_until = null,
                                    updated_at = now()
                                where event_id = :id
                                """)
                        .param("id", event.eventId())
                        .update();
                yield ClaimResult.of(decision);
            }
            default -> ClaimResult.of(decision);
        };
    }

    @Transactional
    public boolean markInFlight(String eventId, UUID token, double leaseSeconds) {
        return jdbc.sql("""
                        update inbox_events
                        set status = 'IN_FLIGHT',
                            attempts = attempts + 1,
                            locked_until = now() + make_interval(secs => cast(:lease as double precision)),
                            reason = null,
                            detail = null,
                            updated_at = now()
                        where event_id = :id
                          and status = 'RECEIVED'
                          and lease_token = :token
                          and locked_until > now()
                        """)
                .param("id", eventId)
                .param("token", token)
                .param("lease", leaseSeconds)
                .update() == 1;
    }

    @Transactional
    public boolean completeSuccess(String eventId, String coreReference) {
        return jdbc.sql("""
                        update inbox_events
                        set status = 'DONE',
                            core_reference = :ref,
                            reason = case when status = 'UNKNOWN' then 'RESOLVED_BY_LATE_RESPONSE' end,
                            detail = null,
                            lease_token = null,
                            locked_until = null,
                            updated_at = now()
                        where event_id = :id
                          and status in ('IN_FLIGHT', 'UNKNOWN')
                        """)
                .param("id", eventId)
                .param("ref", coreReference)
                .update() == 1;
    }

    @Transactional
    public boolean completeRejected(String eventId, String detail) {
        return jdbc.sql("""
                        update inbox_events
                        set status = 'FAILED',
                            reason = 'CORE_REJECTED',
                            detail = :detail,
                            lease_token = null,
                            locked_until = null,
                            updated_at = now()
                        where event_id = :id
                          and status in ('IN_FLIGHT', 'UNKNOWN')
                        """)
                .param("id", eventId)
                .param("detail", detail)
                .update() == 1;
    }

    @Transactional
    public boolean releaseNotSent(String eventId, UUID token, String detail, int maxAttempts, Duration backoff) {
        return jdbc.sql("""
                        update inbox_events
                        set status = case when attempts >= :max then 'FAILED' else 'RECEIVED' end,
                            reason = case when attempts >= :max then 'NOT_SENT_MAX_ATTEMPTS' else 'NOT_SENT' end,
                            detail = :detail,
                            lease_token = null,
                            locked_until = null,
                            next_attempt_at = now() + make_interval(
                                secs => cast(:backoff as double precision) * power(2, least(attempts - 1, 10))),
                            updated_at = now()
                        where event_id = :id
                          and status = 'IN_FLIGHT'
                          and lease_token = :token
                        """)
                .param("id", eventId)
                .param("token", token)
                .param("detail", detail)
                .param("max", maxAttempts)
                .param("backoff", backoff.toMillis() / 1000.0)
                .update() == 1;
    }

    @Transactional
    public boolean markAmbiguous(String eventId, UUID token, String detail) {
        return jdbc.sql("""
                        update inbox_events
                        set status = 'UNKNOWN',
                            reason = 'CORE_AMBIGUOUS',
                            detail = :detail,
                            lease_token = null,
                            locked_until = null,
                            updated_at = now()
                        where event_id = :id
                          and status = 'IN_FLIGHT'
                          and lease_token = :token
                        """)
                .param("id", eventId)
                .param("token", token)
                .param("detail", detail)
                .update() == 1;
    }

    @Transactional
    public int parkExpiredInFlight() {
        return jdbc.sql("""
                        update inbox_events
                        set status = 'UNKNOWN',
                            reason = 'LEASE_EXPIRED_IN_FLIGHT',
                            detail = 'Lease expirou com chamada ao core iniciada e sem resultado registrado',
                            lease_token = null,
                            locked_until = null,
                            updated_at = now()
                        where status = 'IN_FLIGHT'
                          and locked_until < now()
                        """)
                .update();
    }

    @Transactional
    public List<Claim> claimDueReceived(String owner, double leaseSeconds, int limit) {
        return jdbc.sql("""
                        update inbox_events e
                        set lease_token = gen_random_uuid(),
                            owner = :owner,
                            locked_until = now() + make_interval(secs => cast(:lease as double precision)),
                            updated_at = now()
                        where e.event_id in (
                            select event_id
                            from inbox_events
                            where status = 'RECEIVED'
                              and (locked_until is null or locked_until < now())
                              and next_attempt_at <= now()
                            order by next_attempt_at
                            limit :limit
                            for update skip locked)
                        returning e.event_id, e.lease_token, e.payload
                        """)
                .param("owner", owner)
                .param("lease", leaseSeconds)
                .param("limit", limit)
                .query((rs, n) -> new Claim(
                        rs.getString("event_id"),
                        rs.getObject("lease_token", UUID.class),
                        fromJson(rs.getString("payload"))))
                .list();
    }

    @Transactional
    public boolean resolve(String eventId, Resolution resolution, String note) {
        String sql = switch (resolution) {
            case DONE -> """
                    update inbox_events
                    set status = 'DONE', reason = 'MANUAL_DONE', resolution_note = :note,
                        owner = 'manual', updated_at = now()
                    where event_id = :id and status = 'UNKNOWN'
                    """;
            case FAILED -> """
                    update inbox_events
                    set status = 'FAILED', reason = 'MANUAL_FAILED', resolution_note = :note,
                        owner = 'manual', updated_at = now()
                    where event_id = :id and status = 'UNKNOWN'
                    """;
            case RETRY -> """
                    update inbox_events
                    set status = 'RECEIVED', reason = 'MANUAL_RETRY', resolution_note = :note,
                        owner = 'manual', lease_token = null, locked_until = null,
                        next_attempt_at = now(), updated_at = now()
                    where event_id = :id and status = 'UNKNOWN'
                    """;
        };
        return jdbc.sql(sql).param("id", eventId).param("note", note).update() == 1;
    }

    @Transactional(readOnly = true)
    public Optional<InboxEvent> find(String eventId) {
        return jdbc.sql("select * from inbox_events where event_id = :id")
                .param("id", eventId)
                .query(EVENT_MAPPER)
                .optional();
    }

    @Transactional(readOnly = true)
    public List<InboxEvent> listByStatus(InboxStatus status, int limit) {
        return jdbc.sql("""
                        select * from inbox_events
                        where status = :status
                        order by updated_at
                        limit :limit
                        """)
                .param("status", status.name())
                .param("limit", limit)
                .query(EVENT_MAPPER)
                .list();
    }

    @Transactional(readOnly = true)
    public List<InboxTransition> transitions(String eventId) {
        return jdbc.sql("""
                        select from_status, to_status, reason, owner, attempts, at
                        from inbox_transitions
                        where event_id = :id
                        order by id
                        """)
                .param("id", eventId)
                .query((rs, n) -> new InboxTransition(
                        rs.getString("from_status") == null ? null : InboxStatus.valueOf(rs.getString("from_status")),
                        InboxStatus.valueOf(rs.getString("to_status")),
                        rs.getString("reason"),
                        rs.getString("owner"),
                        rs.getInt("attempts"),
                        instant(rs, "at")))
                .list();
    }

    @Transactional(readOnly = true)
    public Map<InboxStatus, Long> countByStatus() {
        Map<InboxStatus, Long> counts = new EnumMap<>(InboxStatus.class);
        for (InboxStatus status : InboxStatus.values()) {
            counts.put(status, 0L);
        }
        jdbc.sql("select status, count(*) as total from inbox_events group by status")
                .query(rs -> {
                    counts.put(InboxStatus.valueOf(rs.getString("status")), rs.getLong("total"));
                });
        return counts;
    }

    @Transactional(readOnly = true)
    public double oldestAgeSeconds(InboxStatus status) {
        return jdbc.sql("""
                        select coalesce(extract(epoch from now() - min(updated_at)), 0) as age
                        from inbox_events
                        where status = :status
                        """)
                .param("status", status.name())
                .query(Double.class)
                .single();
    }

    @Transactional
    public int purgeTerminal(Duration retention) {
        return jdbc.sql("""
                        delete from inbox_events
                        where status in ('DONE', 'FAILED')
                          and updated_at < now() - make_interval(secs => cast(:secs as double precision))
                        """)
                .param("secs", (double) retention.toSeconds())
                .update();
    }

    private String toJson(PagamentoEvent event) {
        try {
            return mapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Evento não serializável", e);
        }
    }

    private PagamentoEvent fromJson(String payload) {
        try {
            return mapper.readValue(payload, PagamentoEvent.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Payload inválido na inbox", e);
        }
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        OffsetDateTime value = rs.getObject(column, OffsetDateTime.class);
        return value == null ? null : value.toInstant();
    }

    private static final RowMapper<InboxEvent> EVENT_MAPPER = (rs, n) -> new InboxEvent(
            rs.getString("event_id"),
            InboxStatus.valueOf(rs.getString("status")),
            rs.getString("payload"),
            rs.getString("owner"),
            instant(rs, "locked_until"),
            rs.getInt("attempts"),
            instant(rs, "next_attempt_at"),
            rs.getString("reason"),
            rs.getString("detail"),
            rs.getString("core_reference"),
            rs.getString("resolution_note"),
            instant(rs, "created_at"),
            instant(rs, "updated_at"));
}
