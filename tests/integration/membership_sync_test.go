package integration

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"price-crawler/services/catalog-comparison/internal/messaging"
)

const membershipSyncQueue = "catalog.membership.snapshot.requested.v1"
const membershipChangesQueue = "catalog.membership.changes.requested.v1"
const groupCreateQueue = "comparison.group.create.v1"
const groupAddQueue = "comparison.group.products.add.v1"

type membershipReply struct {
	Status        string             `json:"status"`
	Mode          string             `json:"mode"`
	SnapshotToken string             `json:"snapshot_token"`
	Watermark     int64              `json:"watermark"`
	NextCursor    *string            `json:"next_cursor"`
	Complete      bool               `json:"complete"`
	Items         []json.RawMessage  `json:"items"`
	Through       int64              `json:"through_sequence"`
	Changes       []membershipChange `json:"changes"`
}

type membershipChange struct {
	Sequence    int64           `json:"sequence"`
	EventType   string          `json:"event_type"`
	Group       json.RawMessage `json:"group"`
	Association json.RawMessage `json:"association"`
}

type membershipRow struct {
	GroupID    string   `json:"group_id"`
	OfferID    string   `json:"offer_id"`
	Revision   int64    `json:"revision"`
	State      string   `json:"state"`
	ProductIDs []string `json:"product_ids"`
}

type membershipTestClient struct {
	t             *testing.T
	channel       *amqp.Channel
	confirmations <-chan amqp.Confirmation
	returned      <-chan amqp.Return
	userID        string
	organization  string
	manager       string
	foreignOrg    string
}

func newMembershipTestClient(t *testing.T) *membershipTestClient {
	t.Helper()
	broker := os.Getenv("CATALOG_MEMBERSHIP_SYNC_TEST_RABBITMQ_URL")
	organization := os.Getenv("CATALOG_MEMBERSHIP_SYNC_TEST_ORGANIZATION_ID")
	manager := os.Getenv("CATALOG_MEMBERSHIP_SYNC_TEST_MANAGER_SUBJECT")
	foreignOrg := os.Getenv("CATALOG_MEMBERSHIP_SYNC_TEST_FOREIGN_ORGANIZATION_ID")
	if broker == "" || organization == "" || manager == "" || foreignOrg == "" {
		t.Skip("set CATALOG_MEMBERSHIP_SYNC_TEST_RABBITMQ_URL, _ORGANIZATION_ID, _MANAGER_SUBJECT, and _FOREIGN_ORGANIZATION_ID for the disposable integration environment")
	}
	parsed, err := url.Parse(broker)
	if err != nil || parsed.User == nil || parsed.User.Username() == "" {
		t.Fatal("membership sync test broker URL must include its publishing user")
	}
	connection, err := amqp.Dial(broker)
	if err != nil {
		t.Fatalf("connect to catalog membership test broker: %v", err)
	}
	channel, err := connection.Channel()
	if err != nil {
		_ = connection.Close()
		t.Fatalf("open membership sync test channel: %v", err)
	}
	if err = channel.Confirm(false); err != nil {
		_ = channel.Close()
		_ = connection.Close()
		t.Fatalf("enable membership test publisher confirms: %v", err)
	}
	confirmations := channel.NotifyPublish(make(chan amqp.Confirmation, 1))
	returned := channel.NotifyReturn(make(chan amqp.Return, 1))
	t.Cleanup(func() { _ = channel.Close(); _ = connection.Close() })
	return &membershipTestClient{t: t, channel: channel, confirmations: confirmations, returned: returned, userID: parsed.User.Username(), organization: organization, manager: manager, foreignOrg: foreignOrg}
}

func newTestUUID(t *testing.T) string {
	t.Helper()
	value, err := uuid.NewRandom()
	if err != nil {
		t.Fatal(err)
	}
	return value.String()
}

func (client *membershipTestClient) request(messageType, queue, organization, idempotencyKey string, payload any) json.RawMessage {
	client.t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		client.t.Fatal(err)
	}
	messageID, correlationID := newTestUUID(client.t), newTestUUID(client.t)
	request := messaging.Envelope{
		MessageID: messageID, MessageType: messageType, SchemaVersion: 1,
		OccurredAt: time.Now().UTC(), Organization: organization,
		CorrelationID: correlationID, IdempotencyKey: idempotencyKey, Payload: raw,
	}
	rawEnvelope, err := request.Encode()
	if err != nil {
		client.t.Fatalf("encode %s request: %v", messageType, err)
	}
	replyQueue, err := client.channel.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		client.t.Fatalf("declare %s reply queue: %v", messageType, err)
	}
	replies, err := client.channel.Consume(replyQueue.Name, "", true, true, false, false, nil)
	if err != nil {
		client.t.Fatalf("consume %s replies: %v", messageType, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err = client.channel.PublishWithContext(ctx, "", queue, true, false, amqp.Publishing{
		ContentType: "application/json", UserId: client.userID, DeliveryMode: amqp.Persistent,
		MessageId: messageID, CorrelationId: correlationID, ReplyTo: replyQueue.Name,
		Type: messageType, Body: rawEnvelope,
	}); err != nil {
		client.t.Fatalf("publish %s request: %v", messageType, err)
	}
	select {
	case confirmation, ok := <-client.confirmations:
		if !ok || !confirmation.Ack {
			client.t.Fatalf("broker did not confirm %s request", messageType)
		}
	case <-ctx.Done():
		client.t.Fatalf("waiting for %s publish confirmation: %v", messageType, ctx.Err())
	}
	select {
	case unroutable := <-client.returned:
		client.t.Fatalf("%s request unroutable to %s", messageType, unroutable.RoutingKey)
	default:
	}
	var reply messaging.Envelope
	select {
	case delivery, ok := <-replies:
		if !ok {
			client.t.Fatalf("%s reply queue closed", messageType)
		}
		reply, err = messaging.Decode(delivery.Body)
		if err != nil {
			client.t.Fatalf("decode %s reply: %v", messageType, err)
		}
		if delivery.CorrelationId != correlationID {
			client.t.Fatalf("%s reply correlation=%s want=%s", messageType, delivery.CorrelationId, correlationID)
		}
	case <-ctx.Done():
		client.t.Fatalf("waiting for %s reply: %v", messageType, ctx.Err())
	}
	if reply.CorrelationID != correlationID || reply.Organization != organization || reply.CausationID != messageID {
		client.t.Fatalf("reply envelope crossed request scope: request=%+v reply=%+v", request, reply)
	}
	return reply.Payload
}

func (client *membershipTestClient) snapshot(organization, token string, cursor *string, limit int) membershipReply {
	client.t.Helper()
	payload := map[string]any{"mode": "snapshot", "limit": limit}
	if token != "" {
		payload["snapshot_token"] = token
	}
	if cursor != nil {
		payload["page_cursor"] = *cursor
	}
	raw := client.request("catalog.membership.snapshot.requested", membershipSyncQueue, organization, "", payload)
	var result membershipReply
	if err := json.Unmarshal(raw, &result); err != nil {
		client.t.Fatalf("decode snapshot result: %v", err)
	}
	if result.Mode != "snapshot" {
		client.t.Fatalf("snapshot responder returned mode %q", result.Mode)
	}
	return result
}

func (client *membershipTestClient) changes(after int64, limit int) membershipReply {
	client.t.Helper()
	raw := client.request("catalog.membership.changes.requested", membershipChangesQueue, client.organization, "", map[string]any{
		"mode": "changes", "after_sequence": after, "limit": limit,
	})
	var result membershipReply
	if err := json.Unmarshal(raw, &result); err != nil {
		client.t.Fatalf("decode changes result: %v", err)
	}
	if result.Mode != "changes" {
		client.t.Fatalf("changes responder returned mode %q", result.Mode)
	}
	return result
}

func (client *membershipTestClient) createGroup(productID, name string) string {
	client.t.Helper()
	key := "membership-sync-test-" + newTestUUID(client.t)
	raw := client.request("comparison.group.create", groupCreateQueue, client.organization, key, map[string]any{
		"actor":  map[string]string{"subject": client.manager, "role": "manager"},
		"action": "group.create", "name": name, "product_ids": []string{productID},
	})
	var result struct {
		Status   string `json:"status"`
		GroupID  string `json:"group_id"`
		Revision int64  `json:"revision"`
		State    string `json:"state"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		client.t.Fatalf("decode group creation result: %v", err)
	}
	if result.Status != "ok" || result.GroupID == "" || result.Revision != 1 || result.State != "draft" {
		client.t.Fatalf("create group result=%+v", result)
	}
	return result.GroupID
}

func (client *membershipTestClient) addProduct(groupID, productID string, expectedRevision int64) int64 {
	client.t.Helper()
	key := "membership-sync-test-" + newTestUUID(client.t)
	raw := client.request("comparison.group.products.add", groupAddQueue, client.organization, key, map[string]any{
		"actor":  map[string]string{"subject": client.manager, "role": "manager"},
		"action": "group.products.add", "group_id": groupID,
		"expected_revision": expectedRevision, "product_ids": []string{productID},
	})
	var result struct {
		Status   string `json:"status"`
		GroupID  string `json:"group_id"`
		Revision int64  `json:"revision"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		client.t.Fatalf("decode group add result: %v", err)
	}
	if result.Status != "ok" || result.GroupID != groupID || result.Revision != expectedRevision+1 {
		client.t.Fatalf("add product result=%+v", result)
	}
	return result.Revision
}

func collectSnapshot(client *membershipTestClient, first membershipReply) []json.RawMessage {
	client.t.Helper()
	if first.Status != "ok" || first.SnapshotToken == "" || first.Watermark < 0 {
		client.t.Fatalf("snapshot start result=%+v", first)
	}
	rows := append([]json.RawMessage{}, first.Items...)
	page := first
	for !page.Complete {
		if page.NextCursor == nil || *page.NextCursor == "" {
			client.t.Fatalf("incomplete snapshot has no next cursor: %+v", page)
		}
		page = client.snapshot(client.organization, first.SnapshotToken, page.NextCursor, 1)
		if page.Status != "ok" || page.SnapshotToken != first.SnapshotToken || page.Watermark != first.Watermark {
			client.t.Fatalf("snapshot page changed token or watermark: %+v", page)
		}
		rows = append(rows, page.Items...)
	}
	if page.NextCursor != nil {
		client.t.Fatalf("complete snapshot unexpectedly returned a cursor: %+v", page)
	}
	return rows
}

func snapshotRowsByKey(t *testing.T, rows []json.RawMessage) map[string]membershipRow {
	t.Helper()
	indexed := make(map[string]membershipRow, len(rows))
	for _, raw := range rows {
		var row membershipRow
		if err := json.Unmarshal(raw, &row); err != nil {
			t.Fatal(err)
		}
		key := row.GroupID
		if key == "" {
			key = row.OfferID
		}
		if key == "" || row.Revision < 1 {
			t.Fatalf("snapshot row has no entity ID or positive revision: %s", raw)
		}
		if (row.GroupID == "") == (row.OfferID == "") {
			t.Fatalf("snapshot row must contain exactly one group or association: %s", raw)
		}
		if row.GroupID != "" && (row.State != "draft" && row.State != "validated" || row.ProductIDs == nil) {
			t.Fatalf("snapshot group row is incomplete: %s", raw)
		}
		if _, duplicate := indexed[key]; duplicate {
			t.Fatalf("snapshot contains duplicate entity %q", key)
		}
		indexed[key] = row
	}
	return indexed
}

func TestMembershipSnapshotFrozenPagesTenantScopeAndJournalRecovery(t *testing.T) {
	client := newMembershipTestClient(t)
	productID := os.Getenv("CATALOG_MEMBERSHIP_SYNC_TEST_PRODUCT_ID")
	secondProductID := os.Getenv("CATALOG_MEMBERSHIP_SYNC_TEST_SECOND_PRODUCT_ID")
	if productID == "" || secondProductID == "" {
		t.Skip("set CATALOG_MEMBERSHIP_SYNC_TEST_PRODUCT_ID and _SECOND_PRODUCT_ID to canonical products in the disposable tenant")
	}
	groupOne := client.createGroup(productID, "membership sync fixture one "+newTestUUID(t))
	_ = client.createGroup(productID, "membership sync fixture two "+newTestUUID(t))
	baselineStart := client.snapshot(client.organization, "", nil, 1)
	if baselineStart.Status != "ok" || baselineStart.Complete || baselineStart.NextCursor == nil {
		t.Fatalf("fixture must yield multiple snapshot pages: %+v", baselineStart)
	}
	baselineRows := snapshotRowsByKey(t, collectSnapshot(client, baselineStart))
	if _, ok := baselineRows[groupOne]; !ok {
		t.Fatalf("created group %s absent from completed snapshot", groupOne)
	}
	associationCount := 0
	for _, row := range baselineRows {
		if row.OfferID != "" {
			associationCount++
		}
	}
	if associationCount == 0 {
		t.Fatal("membership snapshot fixture must include at least one offer association to check consistent group/association pages")
	}

	pageOne := client.snapshot(client.organization, "", nil, 1)
	if pageOne.Status != "ok" || pageOne.Complete || pageOne.NextCursor == nil {
		t.Fatalf("concurrent-edit fixture must yield multiple pages: %+v", pageOne)
	}
	foreignPage := client.snapshot(client.foreignOrg, pageOne.SnapshotToken, pageOne.NextCursor, 1)
	if foreignPage.Status != "forbidden" || len(foreignPage.Items) != 0 {
		t.Fatalf("foreign organization received snapshot rows: %+v", foreignPage)
	}
	newRevision := client.addProduct(groupOne, secondProductID, 1)
	if newRevision != 2 {
		t.Fatalf("group revision after concurrent edit=%d want=2", newRevision)
	}
	frozenRows := snapshotRowsByKey(t, collectSnapshot(client, pageOne))
	if !reflect.DeepEqual(baselineRows, frozenRows) {
		t.Fatalf("frozen snapshot changed during concurrent group edit: baseline=%v frozen=%v", baselineRows, frozenRows)
	}

	changes := client.changes(pageOne.Watermark, 100)
	if changes.Status != "ok" || changes.Through < pageOne.Watermark {
		t.Fatalf("change read failed: %+v", changes)
	}
	expectedSequence := pageOne.Watermark + 1
	foundGroupRevision := false
	for _, change := range changes.Changes {
		if change.Sequence != expectedSequence {
			t.Fatalf("journal gap or duplicate: sequence=%d want=%d", change.Sequence, expectedSequence)
		}
		expectedSequence++
		switch change.EventType {
		case "comparison.group.changed":
			if len(change.Group) == 0 || len(change.Association) != 0 {
				t.Fatalf("group event is partial or includes an association: %+v", change)
			}
			var group membershipRow
			if err := json.Unmarshal(change.Group, &group); err != nil {
				t.Fatal(err)
			}
			if group.GroupID == groupOne && group.Revision == newRevision {
				foundGroupRevision = true
			}
		case "catalog.offer.association.changed":
			if len(change.Association) == 0 || len(change.Group) != 0 {
				t.Fatalf("association event is partial or includes a group: %+v", change)
			}
			var association membershipRow
			if err := json.Unmarshal(change.Association, &association); err != nil || association.OfferID == "" || association.Revision < 1 {
				t.Fatalf("invalid association revision in journal event: %+v err=%v", change, err)
			}
		default:
			t.Fatalf("unexpected journal event type %q", change.EventType)
		}
	}
	if changes.Through != expectedSequence-1 {
		t.Fatalf("through_sequence=%d does not match contiguous changes ending at %d", changes.Through, expectedSequence-1)
	}
	if !foundGroupRevision {
		t.Fatalf("journal omitted current group revision %d for %s", newRevision, groupOne)
	}

	restarted := client.snapshot(client.organization, "", nil, 100)
	if restarted.Status != "ok" || !restarted.Complete {
		t.Fatalf("fresh snapshot after journal recovery did not complete: %+v", restarted)
	}
	latestRows := snapshotRowsByKey(t, restarted.Items)
	latestGroup, ok := latestRows[groupOne]
	if !ok {
		t.Fatalf("restarted snapshot omitted group %s", groupOne)
	}
	productSet := map[string]bool{}
	for _, id := range latestGroup.ProductIDs {
		productSet[id] = true
	}
	if latestGroup.Revision != newRevision || len(productSet) != 2 || !productSet[productID] || !productSet[secondProductID] {
		t.Fatalf("restarted snapshot did not atomically expose the latest group membership: %+v", latestGroup)
	}
}

func TestMembershipSnapshotExpiredCursorRequiresRestart(t *testing.T) {
	client := newMembershipTestClient(t)
	productID := os.Getenv("CATALOG_MEMBERSHIP_SYNC_TEST_PRODUCT_ID")
	if productID == "" {
		t.Skip("set CATALOG_MEMBERSHIP_SYNC_TEST_PRODUCT_ID to a canonical product in the disposable tenant")
	}
	_ = client.createGroup(productID, "membership expiry fixture one "+newTestUUID(t))
	_ = client.createGroup(productID, "membership expiry fixture two "+newTestUUID(t))
	first := client.snapshot(client.organization, "", nil, 1)
	if first.Status != "ok" || first.Complete || first.NextCursor == nil || first.SnapshotToken == "" {
		t.Fatalf("expiry fixtures must yield an incomplete snapshot; got status=%s complete=%t", first.Status, first.Complete)
	}
	t.Log("waiting for the contract's 120-second snapshot TTL to expire")
	time.Sleep(121 * time.Second)
	expired := client.snapshot(client.organization, first.SnapshotToken, first.NextCursor, 1)
	if expired.Status != "expired" || len(expired.Items) != 0 {
		t.Fatalf("expired cursor did not return expired without rows: %+v", expired)
	}
	restarted := client.snapshot(client.organization, "", nil, 100)
	if restarted.Status != "ok" || !restarted.Complete || restarted.SnapshotToken == first.SnapshotToken || restarted.Watermark < first.Watermark {
		t.Fatalf("fresh snapshot did not restart recovery after expiry: first=%+v restarted=%+v", first, restarted)
	}
}

func TestMembershipChangeCursorSequenceIsContiguousAcrossPages(t *testing.T) {
	client := newMembershipTestClient(t)
	sequenceText := os.Getenv("CATALOG_MEMBERSHIP_SYNC_TEST_AFTER_SEQUENCE")
	if sequenceText == "" {
		t.Skip("set CATALOG_MEMBERSHIP_SYNC_TEST_AFTER_SEQUENCE to an available journal cursor")
	}
	after, err := strconv.ParseInt(sequenceText, 10, 64)
	if err != nil || after < 0 {
		t.Fatalf("invalid after-sequence %q", sequenceText)
	}
	page := client.changes(after, 1)
	if page.Status != "ok" {
		t.Fatalf("change read status=%s", page.Status)
	}
	expected := after + 1
	for _, change := range page.Changes {
		if change.Sequence != expected {
			t.Fatalf("first journal page sequence=%d want=%d", change.Sequence, expected)
		}
		expected++
	}
	if page.Through != expected-1 || (page.Complete && page.Through != page.Watermark) {
		t.Fatalf("invalid bounded journal page: %+v", page)
	}
}
