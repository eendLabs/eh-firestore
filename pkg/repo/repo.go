package repo

import (
	"context"
	"fmt"

	"errors"

	"cloud.google.com/go/firestore"
	"github.com/eendLabs/eh-firestore/pkg/share"
	"github.com/google/uuid"
	eh "github.com/looplab/eventhorizon"
)

//Repo config of the repository
type Repo struct {
	client  *firestore.Client
	config  *share.Config
	encoder share.Encoder
}

//NewRepo returns a new instance of a repo
func NewRepo(config *share.Config) (*Repo, error) {
	config.ProvideDefaults()

	client, err := firestore.NewClient(context.TODO(), config.ProjectID)
	if err != nil {
		return nil, errors.New(fmt.Sprintf(share.ErrCouldNotDialDB, err))
	}

	return NewRepoWithClient(config, client)
}

//NewRepoWithClient returns a new instance of a repo with a client
func NewRepoWithClient(config *share.Config, client *firestore.Client) (*Repo, error) {
	if client == nil {
		return nil, share.ErrNoDBClient
	}

	r := &Repo{
		client:  client,
		config:  config,
		encoder: &share.JsonEncoder{},
	}

	return r, nil
}

func (r *Repo) SetEntityFactory(func() eh.Entity) {

}

func (r *Repo) Parent() eh.ReadRepo {
	return nil
}

func (r *Repo) Close(ctx context.Context) {}

func (r *Repo) Clear(ctx context.Context) error {
	return nil
}

func (r *Repo) Save(ctx context.Context, model eh.Entity) error {
	return nil
}

func (r *Repo) Remove(ctx context.Context, id uuid.UUID) error {
	return nil
}

func (r *Repo) Find(ctx context.Context, id uuid.UUID) (eh.Entity, error) {
	return nil, eh.RepoError{
		Err: eh.ErrEntityNotFound, BaseErr: errors.New("err"),
	}
}

// func (r *Repo) FindCustom(ctx context.Context) {}

func (r *Repo) FindAll(ctx context.Context) ([]eh.Entity, error) {
	return nil, nil
}

func Repository(p eh.ReadRepo) *Repo {
	return nil
}
