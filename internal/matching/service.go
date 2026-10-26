package matching

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"
)

// MatchingWeights defines the dynamic coefficients applied during candidate scoring.
type MatchingWeights struct {
	VectorWeight   float64 `json:"vector_weight"`
	RankWeight     float64 `json:"rank_weight"`
	ScheduleWeight float64 `json:"schedule_weight"`
	TraitWeight    float64 `json:"trait_weight"`
}

// PersonaConfig stores criteria and weights for persona-driven matchmaking.
type PersonaConfig struct {
	ID              uuid.UUID       `json:"id"`
	Slug            string          `json:"slug"`
	SupportedGames  []string        `json:"supported_games"`
	SupportedGenres []string        `json:"supported_genres"`
	Weights         MatchingWeights `json:"weights"`
}

// MatchCandidate represents an evaluated user profile matched against the source user.
type MatchCandidate struct {
	TargetUserID       uuid.UUID `json:"target_user_id"`
	DisplayName        string    `json:"display_name"`
	AvatarURL          string    `json:"avatar_url"`
	MainGames          []string  `json:"main_games"`
	RankSelf           string    `json:"rank_self"`
	TiltResistance     int       `json:"tilt_resistance"`
	ToxicityTolerance  int       `json:"toxicity_tolerance"`
	Pace               string    `json:"pace"`
	WeeklyHours        int       `json:"weekly_hours"`
	CosineSimilarity   float64   `json:"cosine_similarity"`
	ComputedScore      float64   `json:"computed_score"`
	ScoreBreakdownJSON []byte    `json:"score_breakdown_json"`
}

type sourceProfileContext struct {
	TiltResistance    int
	ToxicityTolerance int
	Pace              string
	WeeklyHours       int
}

// Service executes 2-stage matchmaking retrieval:
// 1. Relational SQL filtering + pgvector cosine similarity ranking.
// 2. In-memory psychological and gaming trait heuristic scoring.
type Service struct {
	db     *pgxpool.Pool
	logger *slog.Logger
}

// NewService constructs a new matchmaking Service.
func NewService(db *pgxpool.Pool, logger *slog.Logger) *Service {
	return &Service{
		db:     db,
		logger: logger,
	}
}

// FindAndPersistMatches executes candidate retrieval, calculates scores, and persists proposals.
func (s *Service) FindAndPersistMatches(
	ctx context.Context,
	sourceUserID uuid.UUID,
	personaID uuid.UUID,
	limit int,
) ([]MatchCandidate, error) {
	start := time.Now()

	// 1. Concurrent retrieval of persona configuration and source user profile
	var (
		personaConfig *PersonaConfig
		sourceProfile *sourceProfileContext
	)

	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		var err error
		personaConfig, err = s.getPersonaConfig(gCtx, personaID)
		if err != nil {
			return fmt.Errorf("failed to fetch persona config: %w", err)
		}
		return nil
	})
	g.Go(func() error {
		var err error
		sourceProfile, err = s.getSourceProfile(gCtx, sourceUserID)
		if err != nil {
			return fmt.Errorf("failed to fetch source user profile: %w", err)
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	// 2. SQL Query with relational filtering and pgvector cosine distance calculation (<=>)
	query := `
    WITH source_data AS (
        SELECT 
            COALESCE(pg.main_games, '{}'::text[]) AS main_games,
            p.primary_language,
            p.bio_embedding
        FROM profiles p
        LEFT JOIN profile_gaming pg ON pg.profile_id = p.id
        WHERE p.user_id = $1
    )
    SELECT 
        p.user_id AS target_user_id,
        p.display_name,
        COALESCE(p.metadata->>'avatar_emoji', '🎮') AS avatar_url,
        COALESCE(pg.main_games, '{}'::text[]) AS main_games,
        COALESCE(pg.rank_self, '') AS rank_self,
        COALESCE(pt.tilt_resistance, 3) AS tilt_resistance,
        COALESCE(pt.toxicity_tolerance, 3) AS toxicity_tolerance,
        COALESCE(pt.pace, 'normal') AS pace,
        COALESCE(pg.weekly_hours_approx, 10) AS weekly_hours,
        CASE 
            WHEN p.bio_embedding IS NOT NULL AND s.bio_embedding IS NOT NULL 
            THEN 1.0 - (p.bio_embedding <=> s.bio_embedding)
            ELSE 0.5 
        END AS cosine_sim
    FROM profiles p
    LEFT JOIN profile_gaming pg ON pg.profile_id = p.id
    LEFT JOIN profile_traits pt ON pt.profile_id = p.id
    CROSS JOIN source_data s
    WHERE p.user_id != $1
      AND (
          cardinality(s.main_games) = 0 
          OR pg.main_games IS NULL 
          OR pg.main_games && s.main_games
      )
      AND NOT EXISTS (
          SELECT 1 FROM match_proposals mp
          WHERE mp.source_user_id = $1 
            AND mp.target_user_id = p.user_id
            AND mp.status IN ('accepted', 'declined', 'blocked')
      )
    ORDER BY 
      CASE 
          WHEN p.bio_embedding IS NOT NULL AND s.bio_embedding IS NOT NULL 
          THEN p.bio_embedding <=> s.bio_embedding 
          ELSE 1.0 
      END ASC
    LIMIT 50;
	`

	rows, err := s.db.Query(ctx, query, sourceUserID)
	if err != nil {
		return nil, fmt.Errorf("candidate retrieval query failed: %w", err)
	}
	defer rows.Close()

	var candidates []MatchCandidate
	for rows.Next() {
		var c MatchCandidate
		if err := rows.Scan(
			&c.TargetUserID,
			&c.DisplayName,
			&c.AvatarURL,
			&c.MainGames,
			&c.RankSelf,
			&c.TiltResistance,
			&c.ToxicityTolerance,
			&c.Pace,
			&c.WeeklyHours,
			&c.CosineSimilarity,
		); err != nil {
			return nil, fmt.Errorf("failed to scan candidate row: %w", err)
		}

		s.applyPersonaHeuristic(personaConfig, sourceProfile, &c)
		candidates = append(candidates, c)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error: %w", err)
	}

	// 3. In-memory sort by overall score descending using Go 1.21+ slices.SortFunc
	sortCandidatesDesc(candidates)

	if len(candidates) > limit {
		candidates = candidates[:limit]
	}

	// 4. Atomic batch persistence into match_proposals via pgx.Batch
	if len(candidates) > 0 {
		if err := s.persistProposals(ctx, personaID, sourceUserID, candidates); err != nil {
			return nil, fmt.Errorf("failed to persist proposals: %w", err)
		}
	}

	s.logger.InfoContext(ctx, "completed matchmaking synthesis",
		slog.String("source_user_id", sourceUserID.String()),
		slog.String("persona", personaConfig.Slug),
		slog.Int("matches_found", len(candidates)),
		slog.Duration("latency", time.Since(start)),
	)

	return candidates, nil
}

func (s *Service) applyPersonaHeuristic(
	persona *PersonaConfig,
	src *sourceProfileContext,
	cand *MatchCandidate,
) {
	// Vector similarity score normalized to 0 - 100
	vecScore := math.Max(0, math.Min(100, cand.CosineSimilarity*100.0))

	// Psychological trait distance penalty
	tiltDiff := math.Abs(float64(src.TiltResistance - cand.TiltResistance))
	toxiDiff := math.Abs(float64(src.ToxicityTolerance - cand.ToxicityTolerance))
	traitScore := math.Max(0, 100.0-((tiltDiff*15.0)+(toxiDiff*10.0)))

	// Gaming schedule proximity
	hourDiff := math.Abs(float64(src.WeeklyHours - cand.WeeklyHours))
	schedScore := math.Max(0, 100.0-(hourDiff*3.0))

	// Rank & playstyle alignment
	rankScore := 80.0
	if src.Pace == cand.Pace {
		rankScore = 100.0
	}

	w := persona.Weights
	finalScore := (vecScore * w.VectorWeight) +
		(rankScore * w.RankWeight) +
		(schedScore * w.ScheduleWeight) +
		(traitScore * w.TraitWeight)

	cand.ComputedScore = math.Round(finalScore*10) / 10

	breakdown := map[string]float64{
		"vector_score":   math.Round(vecScore),
		"rank_score":     math.Round(rankScore),
		"schedule_score": math.Round(schedScore),
		"trait_score":    math.Round(traitScore),
	}
	cand.ScoreBreakdownJSON, _ = json.Marshal(breakdown)
}

func (s *Service) persistProposals(
	ctx context.Context,
	personaID uuid.UUID,
	sourceUserID uuid.UUID,
	candidates []MatchCandidate,
) error {
	batch := &pgx.Batch{}

	stmt := `
		INSERT INTO match_proposals (
			persona_id, source_user_id, target_user_id, overall_score, score_breakdown, status
		) VALUES ($1, $2, $3, $4, $5, 'suggested')
		ON CONFLICT (persona_id, source_user_id, target_user_id) 
		DO UPDATE SET 
			overall_score = EXCLUDED.overall_score,
			score_breakdown = EXCLUDED.score_breakdown,
			updated_at = NOW();
	`

	for _, c := range candidates {
		batch.Queue(stmt, personaID, sourceUserID, c.ComputedScore, c.ScoreBreakdownJSON)
	}

	br := s.db.SendBatch(ctx, batch)
	defer br.Close()

	for range candidates {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("batch execution failure on proposal insert: %w", err)
		}
	}
	return nil
}

func (s *Service) getPersonaConfig(ctx context.Context, personaID uuid.UUID) (*PersonaConfig, error) {
	row := s.db.QueryRow(ctx, `
		SELECT id, slug, supported_games, supported_genres, matching_weights
		FROM personas WHERE id = $1
	`, personaID)

	var (
		cfg        PersonaConfig
		weightsRaw []byte
	)

	if err := row.Scan(&cfg.ID, &cfg.Slug, &cfg.SupportedGames, &cfg.SupportedGenres, &weightsRaw); err != nil {
		// Fallback default weights if not in database
		return &PersonaConfig{
			ID:   personaID,
			Slug: "balanced-wingman",
			Weights: MatchingWeights{
				VectorWeight:   0.35,
				RankWeight:     0.25,
				ScheduleWeight: 0.20,
				TraitWeight:    0.20,
			},
		}, nil
	}

	if err := json.Unmarshal(weightsRaw, &cfg.Weights); err != nil {
		return nil, fmt.Errorf("failed to unmarshal matching weights: %w", err)
	}

	return &cfg, nil
}

func (s *Service) getSourceProfile(ctx context.Context, userID uuid.UUID) (*sourceProfileContext, error) {
	row := s.db.QueryRow(ctx, `
		SELECT 
			COALESCE(pt.tilt_resistance, 3),
			COALESCE(pt.toxicity_tolerance, 3),
			COALESCE(pt.pace, 'normal'),
			COALESCE(pg.weekly_hours_approx, 10)
		FROM profiles p
		LEFT JOIN profile_gaming pg ON pg.profile_id = p.id
		LEFT JOIN profile_traits pt ON pt.profile_id = p.id
		WHERE p.user_id = $1
	`, userID)

	var p sourceProfileContext
	if err := row.Scan(&p.TiltResistance, &p.ToxicityTolerance, &p.Pace, &p.WeeklyHours); err != nil {
		return &sourceProfileContext{
			TiltResistance:    3,
			ToxicityTolerance: 3,
			Pace:              "normal",
			WeeklyHours:       10,
		}, nil
	}

	return &p, nil
}

func sortCandidatesDesc(arr []MatchCandidate) {
	slices.SortFunc(arr, func(a, b MatchCandidate) int {
		if a.ComputedScore > b.ComputedScore {
			return -1
		}
		if a.ComputedScore < b.ComputedScore {
			return 1
		}
		return 0
	})
}
