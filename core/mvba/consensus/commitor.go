package consensus

import (
	"bft/mvba/core"
	"bft/mvba/logger"
	"bft/mvba/mempool"
)

type Commitor struct {
	callBack      chan<- struct{}
	commitLeaders map[int64]core.NodeID
	commitBlocks  map[int64]*ConsensusBlock
	blockChan     chan *ConsensusBlock
	curIndex      int64
	pool          *mempool.Mempool
}

func NewCommitor(callBack chan<- struct{}, pool *mempool.Mempool) *Commitor {
	c := &Commitor{
		callBack:      callBack,
		commitLeaders: make(map[int64]core.NodeID),
		commitBlocks:  make(map[int64]*ConsensusBlock),
		blockChan:     make(chan *ConsensusBlock, 100),
		curIndex:      0,
		pool:          pool,
	}

	go func() {
		for block := range c.blockChan {
			//logger.Info.Printf("commit ConsensusBlock epoch %d node %d the length of the payload is %d\n", block.Epoch, block.Proposer, len(block.PayLoads))
			for _, payload := range block.PayLoads {
				if smallblock, err := c.pool.GetBlock(payload); err == nil {
					if smallblock.Batch.Txs != nil {
						logger.Info.Printf("commit Block node %d batch_id %d \n", smallblock.Proposer, smallblock.Batch.ID)
					}
				} else {
					//阻塞提交，等待收到payload
					logger.Error.Printf("get key error\n")
				}
			}
			logger.Info.Printf("commit ConsensusBlock epoch %d node %d\n", block.Epoch, block.Proposer)
			c.callBack <- struct{}{}
		}
	}()

	return c
}

func (c *Commitor) CommitLeader(epoch int64) core.NodeID {
	leader, ok := c.commitLeaders[epoch]
	if !ok {
		return core.NONE
	}
	return leader
}

func (c *Commitor) Commit(epoch int64, leader core.NodeID, block *ConsensusBlock) {
	if epoch < c.curIndex {
		return
	}
	c.commitLeaders[epoch] = leader
	if block == nil {
		return
	}
	c.commitBlocks[epoch] = block
	for {
		if block, ok := c.commitBlocks[c.curIndex]; ok {
			c.blockChan <- block
			delete(c.commitBlocks, c.curIndex)
			c.curIndex++
		} else {
			break
		}
	}
}
