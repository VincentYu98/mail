package repo

import (
	"encoding/json"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/vincentAlen/mail"
)

// --- Domain -> Doc ---

func RewardsToDocs(items []mail.RewardItem) []RewardDoc {
	if len(items) == 0 {
		return nil
	}
	docs := make([]RewardDoc, len(items))
	for i, it := range items {
		docs[i] = RewardDoc{ItemID: it.ItemID, Count: it.Count}
	}
	return docs
}

func TargetToDoc(t mail.Target) TargetDoc {
	doc := TargetDoc{Scope: t.Scope}
	if t.Data != nil {
		raw, err := bson.Marshal(bson.M{"v": t.Data})
		if err == nil {
			var wrapper struct {
				V bson.RawValue `bson:"v"`
			}
			if bson.Unmarshal(raw, &wrapper) == nil {
				doc.Data = wrapper.V
			}
		}
	}
	return doc
}

func MarshalI18nParams(v any) bson.RawValue {
	if v == nil {
		return bson.RawValue{}
	}
	raw, err := bson.Marshal(bson.M{"v": v})
	if err != nil {
		return bson.RawValue{}
	}
	var wrapper struct {
		V bson.RawValue `bson:"v"`
	}
	if bson.Unmarshal(raw, &wrapper) == nil {
		return wrapper.V
	}
	return bson.RawValue{}
}

// --- Doc -> Domain ---

func DocsToRewards(docs []RewardDoc) []mail.RewardItem {
	if len(docs) == 0 {
		return nil
	}
	items := make([]mail.RewardItem, len(docs))
	for i, d := range docs {
		items[i] = mail.RewardItem{ItemID: d.ItemID, Count: d.Count}
	}
	return items
}

func DocToTarget(doc TargetDoc) mail.Target {
	t := mail.Target{Scope: doc.Scope}
	if doc.Data.Type != 0 {
		var raw any
		if err := doc.Data.Unmarshal(&raw); err == nil {
			t.Data = raw
		}
	}
	return t
}

func UnmarshalI18nParams(rv bson.RawValue) any {
	if rv.Type == 0 {
		return nil
	}
	var raw any
	if err := rv.Unmarshal(&raw); err != nil {
		return nil
	}
	return raw
}

func ptrToMs(p *int64) int64 {
	if p == nil {
		return 0
	}
	return *p
}

func UserMailDocToMail(doc UserMailDoc) mail.Mail {
	return mail.Mail{
		ServerID:    doc.ServerID,
		UID:         doc.UID,
		MailID:      doc.MailID,
		Kind:        mail.MailKind(doc.Kind),
		Source:      doc.Source,
		TemplateID:  doc.TemplateID,
		Params:      doc.Params,
		I18nParams:  UnmarshalI18nParams(doc.I18nParams),
		Title:       doc.Title,
		Content:     doc.Content,
		Rewards:     DocsToRewards(doc.Rewards),
		SendAtMs:    doc.SendAt,
		ExpireAtMs:  doc.ExpireAt,
		ReadAtMs:    ptrToMs(doc.ReadAt),
		ClaimedAtMs: ptrToMs(doc.ClaimedAt),
		DeletedAtMs: ptrToMs(doc.DeletedAt),
	}
}

func BroadcastDocToUserMailDoc(bc BroadcastMailDoc, uid int64) UserMailDoc {
	return UserMailDoc{
		ServerID:   bc.ServerID,
		UID:        uid,
		MailID:     bc.MailID,
		Kind:       bc.Kind,
		Source:     bc.Source,
		TemplateID: bc.TemplateID,
		Params:     bc.Params,
		I18nParams: bc.I18nParams,
		Title:      bc.Title,
		Content:    bc.Content,
		Rewards:    bc.Rewards,
		SendAt:     bc.SendAt,
		ExpireAt:   bc.ExpireAt,
		PurgeAt:    bc.PurgeAt,
		Origin:     &OriginDoc{Type: "broadcast", ID: bc.MailID},
	}
}

// TargetDataToJSON is a helper to serialize target data to JSON bytes.
func TargetDataToJSON(data any) []byte {
	if data == nil {
		return nil
	}
	b, _ := json.Marshal(data)
	return b
}
