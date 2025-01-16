// Copied from indigo:api/atproto/repo.go

package agnostic

// schema: com.atproto.repo.

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/bluesky-social/indigo/lex/util"
	"github.com/bluesky-social/indigo/xrpc"
)

// Repo_Create is a "create" in the com.atproto.repo. schema.
//
// Operation which creates a new record.
//
// RECORDTYPE: Repo_Create
type Repo_Create struct {
	LexiconTypeID string           `json:"$type,const=com.atproto.repo.#create" cborgen:"$type,const=com.atproto.repo.#create"`
	Collection    string           `json:"collection" cborgen:"collection"`
	Rkey          *string          `json:"rkey,omitempty" cborgen:"rkey,omitempty"`
	Value         *json.RawMessage `json:"value" cborgen:"value"`
}

// Repo_CreateResult is a "createResult" in the com.atproto.repo. schema.
//
// RECORDTYPE: Repo_CreateResult
type Repo_CreateResult struct {
	LexiconTypeID    string  `json:"$type,const=com.atproto.repo.#createResult" cborgen:"$type,const=com.atproto.repo.#createResult"`
	Cid              string  `json:"cid" cborgen:"cid"`
	Uri              string  `json:"uri" cborgen:"uri"`
	ValidationStatus *string `json:"validationStatus,omitempty" cborgen:"validationStatus,omitempty"`
}

// Repo_Delete is a "delete" in the com.atproto.repo. schema.
//
// Operation which deletes an existing record.
//
// RECORDTYPE: Repo_Delete
type Repo_Delete struct {
	LexiconTypeID string `json:"$type,const=com.atproto.repo.#delete" cborgen:"$type,const=com.atproto.repo.#delete"`
	Collection    string `json:"collection" cborgen:"collection"`
	Rkey          string `json:"rkey" cborgen:"rkey"`
}

// Repo_DeleteResult is a "deleteResult" in the com.atproto.repo. schema.
//
// RECORDTYPE: Repo_DeleteResult
type Repo_DeleteResult struct {
	LexiconTypeID string `json:"$type,const=com.atproto.repo.#deleteResult" cborgen:"$type,const=com.atproto.repo.#deleteResult"`
}

// Repo_Input is the input argument to a com.atproto.repo. call.
type Repo_Input struct {
	// repo: The handle or DID of the repo (aka, current account).
	Repo string `json:"repo" cborgen:"repo"`
	// swapCommit: If provided, the entire operation will fail if the current repo commit CID does not match this value. Used to prevent conflicting repo mutations.
	SwapCommit *string `json:"swapCommit,omitempty" cborgen:"swapCommit,omitempty"`
	// validate: Can be set to 'false' to skip Lexicon schema validation of record data across all operations, 'true' to require it, or leave unset to validate only for known Lexicons.
	Validate *bool                     `json:"validate,omitempty" cborgen:"validate,omitempty"`
	Writes   []*Repo_Input_Writes_Elem `json:"writes" cborgen:"writes"`
}

type Repo_Input_Writes_Elem struct {
	Repo_Create *Repo_Create
	Repo_Update *Repo_Update
	Repo_Delete *Repo_Delete
}

func (t *Repo_Input_Writes_Elem) MarshalJSON() ([]byte, error) {
	if t.Repo_Create != nil {
		t.Repo_Create.LexiconTypeID = "com.atproto.repo.#create"
		return json.Marshal(t.Repo_Create)
	}
	if t.Repo_Update != nil {
		t.Repo_Update.LexiconTypeID = "com.atproto.repo.#update"
		return json.Marshal(t.Repo_Update)
	}
	if t.Repo_Delete != nil {
		t.Repo_Delete.LexiconTypeID = "com.atproto.repo.#delete"
		return json.Marshal(t.Repo_Delete)
	}
	return nil, fmt.Errorf("cannot marshal empty enum")
}
func (t *Repo_Input_Writes_Elem) UnmarshalJSON(b []byte) error {
	typ, err := util.TypeExtract(b)
	if err != nil {
		return err
	}

	switch typ {
	case "com.atproto.repo.#create":
		t.Repo_Create = new(Repo_Create)
		return json.Unmarshal(b, t.Repo_Create)
	case "com.atproto.repo.#update":
		t.Repo_Update = new(Repo_Update)
		return json.Unmarshal(b, t.Repo_Update)
	case "com.atproto.repo.#delete":
		t.Repo_Delete = new(Repo_Delete)
		return json.Unmarshal(b, t.Repo_Delete)

	default:
		return fmt.Errorf("closed enums must have a matching value")
	}
}

// Repo_Output is the output of a com.atproto.repo. call.
type Repo_Output struct {
	Commit  *RepoDefs_CommitMeta        `json:"commit,omitempty" cborgen:"commit,omitempty"`
	Results []*Repo_Output_Results_Elem `json:"results,omitempty" cborgen:"results,omitempty"`
}

type Repo_Output_Results_Elem struct {
	Repo_CreateResult *Repo_CreateResult
	Repo_UpdateResult *Repo_UpdateResult
	Repo_DeleteResult *Repo_DeleteResult
}

func (t *Repo_Output_Results_Elem) MarshalJSON() ([]byte, error) {
	if t.Repo_CreateResult != nil {
		t.Repo_CreateResult.LexiconTypeID = "com.atproto.repo.#createResult"
		return json.Marshal(t.Repo_CreateResult)
	}
	if t.Repo_UpdateResult != nil {
		t.Repo_UpdateResult.LexiconTypeID = "com.atproto.repo.#updateResult"
		return json.Marshal(t.Repo_UpdateResult)
	}
	if t.Repo_DeleteResult != nil {
		t.Repo_DeleteResult.LexiconTypeID = "com.atproto.repo.#deleteResult"
		return json.Marshal(t.Repo_DeleteResult)
	}
	return nil, fmt.Errorf("cannot marshal empty enum")
}
func (t *Repo_Output_Results_Elem) UnmarshalJSON(b []byte) error {
	typ, err := util.TypeExtract(b)
	if err != nil {
		return err
	}

	switch typ {
	case "com.atproto.repo.#createResult":
		t.Repo_CreateResult = new(Repo_CreateResult)
		return json.Unmarshal(b, t.Repo_CreateResult)
	case "com.atproto.repo.#updateResult":
		t.Repo_UpdateResult = new(Repo_UpdateResult)
		return json.Unmarshal(b, t.Repo_UpdateResult)
	case "com.atproto.repo.#deleteResult":
		t.Repo_DeleteResult = new(Repo_DeleteResult)
		return json.Unmarshal(b, t.Repo_DeleteResult)

	default:
		return fmt.Errorf("closed enums must have a matching value")
	}
}

// Repo_Update is a "update" in the com.atproto.repo. schema.
//
// Operation which updates an existing record.
//
// RECORDTYPE: Repo_Update
type Repo_Update struct {
	LexiconTypeID string           `json:"$type,const=com.atproto.repo.#update" cborgen:"$type,const=com.atproto.repo.#update"`
	Collection    string           `json:"collection" cborgen:"collection"`
	Rkey          string           `json:"rkey" cborgen:"rkey"`
	Value         *json.RawMessage `json:"value" cborgen:"value"`
}

// Repo_UpdateResult is a "updateResult" in the com.atproto.repo. schema.
//
// RECORDTYPE: Repo_UpdateResult
type Repo_UpdateResult struct {
	LexiconTypeID    string  `json:"$type,const=com.atproto.repo.#updateResult" cborgen:"$type,const=com.atproto.repo.#updateResult"`
	Cid              string  `json:"cid" cborgen:"cid"`
	Uri              string  `json:"uri" cborgen:"uri"`
	ValidationStatus *string `json:"validationStatus,omitempty" cborgen:"validationStatus,omitempty"`
}

// Repo calls the XRPC method "com.atproto.repo.".
func Repo(ctx context.Context, c *xrpc.Client, input *Repo_Input) (*Repo_Output, error) {
	var out Repo_Output
	if err := c.Do(ctx, xrpc.Procedure, "application/json", "com.atproto.repo.", nil, input, &out); err != nil {
		return nil, err
	}

	return &out, nil
}
