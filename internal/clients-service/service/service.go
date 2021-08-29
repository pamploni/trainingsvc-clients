package service

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sort"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
	"github.com/pedidopago/trainingsvc-clients/protos/pb"
	"github.com/pedidopago/trainingsvc-clients/utils"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Config struct {
	DBCS string
}

func New(ctx context.Context, sv *grpc.Server, config Config) error {

	svc := &Service{}

	// database connection
	db, err := sqlx.Open("mysql", config.DBCS)
	if err != nil {
		return err
	}
	svc.db = db

	go svc.cleanup(ctx) // executa antes de fechar o app

	pb.RegisterClientsServiceServer(sv, svc)

	return nil
}

type Service struct {
	db *sqlx.DB
}

func (s *Service) cleanup(ctx context.Context) {
	<-ctx.Done()
	s.db.Close()
}

var _ pb.ClientsServiceServer = (*Service)(nil) // compile time check if we support the public proto interface

// NewClient creates a new client on the database
func (s *Service) NewClient(ctx context.Context, req *pb.NewClientRequest) (*pb.NewClientResponse, error) {
	//FIXME: this method

	id := utils.SecureID().String()

	cols := make([]string, 0)
	vals := make([]interface{}, 0)

	cols, vals = append(cols, "id"), append(vals, id)
	
	//FIXME: adicionar name
	cols, vals = append(cols, "name"), append(vals, req.Name)
	
	if req.Birthday != 0 {
		cols, vals = append(cols, "birthday"), append(vals, time.Unix(0, req.Birthday))
	}
	
	//FIXME: adicionar score
	if req.Score >= 0 {
		cols, vals = append(cols, "score"), append(vals, req.Score)
	}

	q, args, err := sq.Insert("clients").Columns(cols...).Values(vals...).ToSql()
	if err != nil {
		return nil, err
	}
	
	//FIXME: executar query com s.db.ExecCtx...
	_, err2 := s.db.ExecContext(ctx, q, args...);
	if err2 != nil {
		return nil, err
	}
	
	return &pb.NewClientResponse{
	 	Id: id,
	 }, nil
}

func (s *Service) QueryClients(ctx context.Context, req *pb.QueryClientsRequest) (*pb.QueryClientsResponse, error) {
	rq := sq.Select("id").From("clients")
	if req.Id != nil {
		rq = rq.Where("id", req.Id.Value)
	}
	if req.Name != nil {
		rq = rq.Where("name LIKE ?", req.Name.Value)
	}
	if req.Birthday != nil {
		rq = rq.Where("birthday", req.Birthday)
	}
	if req.Score != nil {
		rq = rq.Where("score", req.Score)
	}

	//FIXME: adicionar created_at
	if req.CreatedAt != nil {
		rq = rq.Where("created_at", req.CreatedAt)
	}

	//FIXME: ordenar por score! (DESC)
	rq = rq.OrderBy("score DESC");

	q, args, err := rq.ToSql()
	if err != nil {
		return nil, err
	}

	log.Println(q);

	ids := make([]string, 0)
	if err := s.db.SelectContext(ctx, &ids, q, args...); err != nil {
		return nil, err
	}

	return &pb.QueryClientsResponse{
		Ids: ids,
	}, nil
}

func (s *Service) GetClients(ctx context.Context, req *pb.GetClientsRequest) (*pb.GetClientsResponse, error) {
	ifids := make([]interface{}, 0, len(req.Ids))
	for _, v := range req.Ids {
		ifids = append(ifids, v)
	}
	q, args, err := sq.Select("id", "name", "birthday", "score", "created_at").From("`clients`").
		Where(fmt.Sprintf("id IN (%s)", sq.Placeholders(len(ifids))), ifids...).ToSql()
	if err != nil {
		return nil, err
	}
	rawclients := []struct {
		ID        string        `db:"id"`
		Name      string        `db:"name"`
		Birthday  sql.NullTime  `db:"birthday"`
		Score     sql.NullInt64 `db:"score"`
		CreatedAt sql.NullTime  `db:"created_at"`
	}{}
	if err := s.db.SelectContext(ctx, &rawclients, q, args...); err != nil {
		return nil, err
	}
	resp := &pb.GetClientsResponse{
		Clients: make([]*pb.Client, 0, len(rawclients)),
	}
	for _, v := range rawclients {
		resp.Clients = append(resp.Clients, &pb.Client{
			Id:       v.ID,
			Name:     v.Name,
			Birthday: v.Birthday.Time.UnixNano(),
			Score:    v.Score.Int64,
			CreatedAt: &timestamppb.Timestamp{},
		})
	}
	return resp, nil
}

//FIXME: implementar NewMatch

func (s *Service) NewMatch(ctx context.Context, req *pb.NewMatchRequest) (*pb.NewMatchResponse, error) {
	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, err
	}
	resp, err := tx.Exec("INSERT INTO client_matches (client_id, score) VALUES (?, ?)", req.ClientId, req.Score); 
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}

	returnedId, err := resp.LastInsertId();
	if err != nil {
		return nil, err
	}

	//FIXME: tx -> UPDATE clients SET score = score + ? WHERE id = ?
	if _, err := tx.Exec("UPDATE clients SET score = score + ? WHERE id = ?", req.Score, req.ClientId); err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	
	tx.Commit()	
	
	//TODO: remover esta linha
	return &pb.NewMatchResponse{ Id: returnedId}, nil; //TODO: trocar linha por implementação
}

func (s *Service) DeleteClient(ctx context.Context, req *pb.DeleteClientRequest) (*pb.DeleteClientResponse, error) {
	//FIXME: implementar DeleteClient()
	if _, err := s.db.Exec("DELETE FROM clients WHERE id = ?", req.Id); err != nil {
		return nil, err
	}
	return &pb.DeleteClientResponse{}, nil
	
}

func (s *Service) DeleteAllClients(ctx context.Context, req *pb.DeleteAllClientsRequest) (*pb.DeleteAllClientsResponse, error) {
	if _, err := s.db.ExecContext(ctx, "DELETE FROM clients"); err != nil {
		return nil, err
	}
	return &pb.DeleteAllClientsResponse{}, nil
}

//Limpar itens duplicados
func removeDuplicateValues(intSlice []string) []string {
	keys := make(map[string]bool)
	list := []string{}

	
	for _, entry := range intSlice {
		 if _, value := keys[entry]; !value {
			  keys[entry] = true
			  list = append(list, entry)
		 }
	}
	return list
}

//FIXME: implementar Sort()
func (s *Service) Sort(ctx context.Context, req *pb.SortRequest) (*pb.SortResponse, error) {
	
	sortMtx := req.Items;

	sort.Strings(sortMtx);

	if (req.RemoveDuplicates) {
		sortMtx = removeDuplicateValues(sortMtx);
	}

	return &pb.SortResponse{ Items: sortMtx}, nil
}
