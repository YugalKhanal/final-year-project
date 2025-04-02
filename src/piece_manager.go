package main

import (
	"log"
	"math/rand"
	"sort"
	"sync"
)

type PieceStatus int

const (
	PieceMissing PieceStatus = iota
	PieceRequested
	PieceReceived
	PieceVerified
)

type PieceInfo struct {
	Index    int
	Size     int
	Hash     string
	Status   PieceStatus
	Peers    map[string]bool // peers that have this piece
	Priority int             // higher number = higher priority
}

// PieceManager Keeps track of which peers are availalble for each piece
type PieceManager struct {
	pieces    map[int]*PieceInfo
	numPieces int
	mu        sync.RWMutex
}

func NewPieceManager(numPieces int, hashes []string) *PieceManager {
	pm := &PieceManager{
		pieces:    make(map[int]*PieceInfo),
		numPieces: numPieces,
	}

	for i := range numPieces {
		pm.pieces[i] = &PieceInfo{
			Index:    i,
			Hash:     hashes[i],
			Status:   PieceMissing,
			Peers:    make(map[string]bool),
			Priority: 0,
		}
	}

	return pm
}

// UpdatePeerPieces updates which pieces a peer has
func (pm *PieceManager) UpdatePeerPieces(peerAddr string, pieceIndices []int) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Add peer to each piece's peer list
	for _, idx := range pieceIndices {
		if piece, exists := pm.pieces[idx]; exists {
			piece.Peers[peerAddr] = true

			// Update priority - rarest pieces get highest priority
			piece.Priority = 1000 - len(piece.Peers) // inverse of peer count
		}
	}
}

// RemovePeer removes a peer from all pieces
func (pm *PieceManager) RemovePeer(peerAddr string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	for _, piece := range pm.pieces {
		delete(piece.Peers, peerAddr)
		if piece.Status == PieceRequested {
			piece.Status = PieceMissing // Reset if piece was requested from this peer
		}
		piece.Priority = 1000 - len(piece.Peers)
	}
}

// GetNextPieces returns the next n pieces to download, prioritizing rarest pieces
// and distributing load across peers
func (pm *PieceManager) GetNextPieces(n int) []PieceInfo {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	// Get all missing or failed pieces
	candidates := make([]PieceInfo, 0)
	for _, piece := range pm.pieces {
		if piece.Status == PieceMissing && len(piece.Peers) > 0 {
			candidates = append(candidates, *piece)
		}
	}

	// If no candidates, try to include requested pieces that might have timed out
	if len(candidates) == 0 {
		for _, piece := range pm.pieces {
			if piece.Status == PieceRequested && len(piece.Peers) > 0 {
				candidates = append(candidates, *piece)
			}
		}
	}

	// Sort by priority (rarest first) and add some randomness to avoid all clients
	// requesting the same pieces simultaneously
	sort.Slice(candidates, func(i, j int) bool {
		// 80% of the time use priority, 20% of the time use random order
		if rand.Float32() < 0.8 {
			return candidates[i].Priority > candidates[j].Priority
		}
		return rand.Intn(2) == 0
	})

	// Return up to n pieces
	if len(candidates) > n {
		candidates = candidates[:n]
	}

	// Mark pieces as requested
	for _, piece := range candidates {
		pm.pieces[piece.Index].Status = PieceRequested
	}

	return candidates
}

// MarkPieceStatus updates the status of a piece
func (pm *PieceManager) MarkPieceStatus(index int, status PieceStatus) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if piece, exists := pm.pieces[index]; exists {
		oldStatus := piece.Status
		piece.Status = status

		// Log status changes for debugging
		if oldStatus != status {
			statusNames := map[PieceStatus]string{
				PieceMissing:   "Missing",
				PieceRequested: "Requested",
				PieceReceived:  "Received",
				PieceVerified:  "Verified",
			}

			log.Printf("Piece %d status changed: %s -> %s",
				index,
				statusNames[oldStatus],
				statusNames[status])
		}
	}
}

// IsComplete checks if all pieces are verified
func (pm *PieceManager) IsComplete() bool {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	totalPieces := len(pm.pieces)
	verifiedCount := 0

	for _, piece := range pm.pieces {
		if piece.Status != PieceVerified {
			return false
		}
		verifiedCount++
	}

	// Debug logging for when we're close to completion
	if verifiedCount > 0 && verifiedCount >= totalPieces-5 {
		log.Printf("Completion check: %d/%d pieces verified", verifiedCount, totalPieces)
	}

	return true
}
