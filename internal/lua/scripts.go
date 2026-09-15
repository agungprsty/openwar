package lua

import _ "embed"

//go:embed token_bucket.lua
var TokenBucket string

//go:embed reserve_stock.lua
var ReserveStock string

//go:embed release_stock.lua
var ReleaseStock string

//go:embed claim_idem.lua
var ClaimIdem string

//go:embed admit_batch.lua
var AdmitBatch string

//go:embed consume_admit.lua
var ConsumeAdmit string
