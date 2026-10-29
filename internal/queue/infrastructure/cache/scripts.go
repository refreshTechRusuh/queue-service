package cache

// Lua script antrian. Redis menjalankan satu script secara UTUH tanpa diselingi
// perintah lain → "cek slot kosong → admit → INCR occupancy" menjadi satu langkah
// atomik. Inilah yang menjamin occupancy tak pernah melebihi bookingRoomCap
// walau ribuan request masuk bersamaan (anti-PHP, Req 23.5).
//
// Konvensi KEYS (semua memakai hash tag {eventId} → satu slot Redis):
//   KEYS[1] queue:{e}          ZSET antrian  (member=token, score=nomor urut datang)
//   KEYS[2] admitted:{e}       ZSET admitted (member=token, score=expiresAt ms)
//   KEYS[3] admission:{e}      counter occupancy
//   KEYS[4] stock:{e}          HASH available per kategori
//   KEYS[5] admission:log:{e}  ZSET jejak admit (untuk laju/ETA)
//   KEYS[6] queue:closed:{e}   flag sold-out
//   KEYS[7] queue:seq:{e}      counter nomor urut datang (score antrian)
// ARGV umum:
//   ARGV[1] prefix key state token ("queue:token:{e}:")
//   ARGV[2] GLOBAL_BOOKING_CAP (0 = tanpa plafon)
//   ARGV[3] TTL booking (ms)
//   ARGV[4] TTL state token (detik)
//
// Score antrian = nomor urut monoton (INCR), BUKAN timestamp: banyak orang bisa
// datang di milidetik yang sama, dan score seri membuat Redis mengurutkan berdasarkan
// teks token — pendatang belakangan bisa terselip di depan. Karena Redis mengeksekusi
// script satu per satu, urutan eksekusi = urutan datang sebenarnya (FCFS ketat).
// Waktu (untuk TTL) diambil dari Redis TIME → satu jam untuk semua instance Cloud Run.

// luaCommon = fungsi bersama yang disisipkan di awal tiap script.
const luaCommon = `
local function nowms()
  local t = redis.call('TIME')
  return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

-- Rumus HARUS identik dengan entity.CapByStock (dijaga test).
local function roomcap(stockKey, globalCap)
  local total = 0
  for _, v in ipairs(redis.call('HVALS', stockKey)) do
    local n = tonumber(v) or 0
    if n > 0 then total = total + n end
  end
  local c
  if total <= 0 then c = 0
  elseif total <= 3 then c = 1
  else c = math.floor(total / 3) end
  if globalCap > 0 and c > globalCap then c = globalCap end
  return c
end

-- Isi slot kosong dari DEPAN antrian sekaligus (gelombang), FCFS.
local function promote(now)
  local q, adm, occ, stock, log = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5]
  local prefix, globalCap = ARGV[1], tonumber(ARGV[2])
  local ttlMs, stateTTL = tonumber(ARGV[3]), tonumber(ARGV[4])
  local free = roomcap(stock, globalCap) - (tonumber(redis.call('GET', occ)) or 0)
  local out = {}
  while free > 0 do
    local p = redis.call('ZPOPMIN', q, 1)
    if #p == 0 then break end
    local tok = p[1]
    local sk = prefix .. tok
    if redis.call('HGET', sk, 'status') == 'QUEUED' then
      local exp = now + ttlMs
      redis.call('HSET', sk, 'status', 'ADMITTED', 'expiresAt', exp)
      redis.call('EXPIRE', sk, stateTTL)
      redis.call('ZADD', adm, exp, tok)
      redis.call('INCR', occ)
      redis.call('ZADD', log, now, tok)
      out[#out + 1] = tok
      free = free - 1
    end
  end
  redis.call('ZREMRANGEBYSCORE', log, '-inf', now - 300000)
  return out
end

-- Lepas kursi satu token bila masih ADMITTED. Return 1 bila benar-benar dilepas.
local function releaseToken(tok, newStatus)
  local sk = ARGV[1] .. tok
  redis.call('ZREM', KEYS[2], tok)
  if redis.call('HGET', sk, 'status') ~= 'ADMITTED' then return 0 end
  redis.call('HSET', sk, 'status', newStatus)
  if (tonumber(redis.call('GET', KEYS[3])) or 0) > 0 then redis.call('DECR', KEYS[3]) end
  return 1
end
`

// luaJoin: ARGV[5]=token, ARGV[6]=queue cap.
// Return: {outcome, nilai, tokenLainYangTerAdmit...}
//
//	ADMITTED → nilai = expiresAt (ms) ; QUEUED → nilai = posisi (1 = terdepan)
const luaJoin = luaCommon + `
local token, queueCap = ARGV[5], tonumber(ARGV[6])

if redis.call('EXISTS', KEYS[6]) == 1 then return {'SOLD_OUT'} end

if queueCap > 0 and redis.call('ZCARD', KEYS[1]) >= queueCap then return {'FULL'} end

-- Semua pendatang masuk antrian dulu, lalu promosi dari DEPAN. Dengan begitu
-- pendatang baru tak pernah menyalip orang yang sudah menunggu (FCFS).
local now = nowms()
local sk = ARGV[1] .. token
redis.call('HSET', sk, 'status', 'QUEUED', 'joinedAt', now)
redis.call('EXPIRE', sk, tonumber(ARGV[4]))
redis.call('ZADD', KEYS[1], redis.call('INCR', KEYS[7]), token)

local promoted = promote(now)

local res
if redis.call('HGET', sk, 'status') == 'ADMITTED' then
  res = {'ADMITTED', redis.call('HGET', sk, 'expiresAt')}
else
  local r = redis.call('ZRANK', KEYS[1], token)
  if r == false then r = 0 end
  res = {'QUEUED', tostring(r + 1)}
end
for _, t in ipairs(promoted) do
  if t ~= token then res[#res + 1] = t end
end
return res
`

// luaPromote: return {tokenTerAdmit...}
const luaPromote = luaCommon + `
return promote(nowms())
`

// luaRelease: ARGV[5]=token. Return {dilepas(0/1), tokenTerAdmit...}
const luaRelease = luaCommon + `
local res = {releaseToken(ARGV[5], 'RELEASED')}
for _, t in ipairs(promote(nowms())) do res[#res + 1] = t end
return res
`

// luaSweep: lepas kursi yang Access Token-nya sudah lewat exp. Return {tokenTerAdmit...}
const luaSweep = luaCommon + `
local now = nowms()
for _, tok in ipairs(redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now, 'LIMIT', 0, 500)) do
  releaseToken(tok, 'EXPIRED')
end
return promote(now)
`

// luaClose (sold out): ARGV[5]=TTL flag tutup (detik). Return jumlah token yang dikeluarkan.
const luaClose = luaCommon + `
redis.call('SET', KEYS[6], '1', 'EX', tonumber(ARGV[5]))
local toks = redis.call('ZRANGE', KEYS[1], 0, -1)
for _, tok in ipairs(toks) do
  redis.call('HSET', ARGV[1] .. tok, 'status', 'SOLD_OUT')
end
redis.call('DEL', KEYS[1])
return #toks
`
