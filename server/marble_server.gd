extends SceneTree
## Marble Racers Days game server: the small program that runs on the Linux server.
## It keeps everyone's player name, who's online, party invites, friends' leaderboard times,
## and it introduces party members' browsers to each other. There are NO party codes:
## a party is found by its host's player (that's what an invite carries). Once introduced,
## browsers race directly with each other (WebRTC); races never go through this server.
##
## Run:  godot --headless --script marble_server.gd -- --port=9080 [--data=/path/to/folder]
##
## Messages are JSON text over a WebSocket.
##   Questions:  {"t":"ask","rid":7,"op":"register","code":..,"name":..}  ->  {"t":"ans","rid":7,"r":...}
##   Parties:    {"t":"host"}  {"t":"join","host":player}  {"t":"leave"}  {"t":"to","to":id,"data":{}}
##               ->  {"t":"room","id":..}  {"t":"peer","id":..}  {"t":"gone","id":..}
##                   {"t":"from","from":id,"data":{}}  {"t":"error","msg":..}

const MAX_PER_PARTY := 16
const ONLINE_SECS := 60.0
const INVITE_SECS := 60.0
const PARTY_KEEP_SECS := 240.0     ## an empty party is kept 4 minutes (people can come back)
const MAX_TIMES := 200000

var _server := TCPServer.new()
var _conns: Array = []             ## [{ws, code, party, id}]
var _parties: Dictionary = {}      ## host player -> {next_id, peers: {id: conn}, empty_since}
var _players: Dictionary = {}      ## player -> {name, seen}
var _invites: Dictionary = {}      ## "to|from" -> {to_code, from_code, from_name, party, state, at}
var _times: Array = []             ## [{track_key, track_name, player_id, player_name, time, at}]
var _data_dir := ""
var _dirty := false
var _save_at := 0.0
var _geo: Geo


func _initialize() -> void:
	var port := 9080
	_data_dir = ProjectSettings.globalize_path("res://").path_join("marble_data")
	for a in OS.get_cmdline_user_args():
		if a.begins_with("--port="):
			port = int(a.substr(7))
		elif a.begins_with("--data="):
			_data_dir = a.substr(7)
	DirAccess.make_dir_recursive_absolute(_data_dir)
	_players = _load("players.json", {})
	_times = _load("times.json", [])
	if _server.listen(port) != OK:
		printerr("marble server: couldn't listen on port %d" % port)
		quit(1)
		return
	print("marble server: listening on port %d, saving to %s" % [port, _data_dir])
	_geo = Geo.new(_data_dir)
	root.add_child(_geo)
	if not OS.get_cmdline_user_args().has("--no-self-update"):
		root.add_child(SelfUpdate.new(self))


func _load(file: String, empty: Variant) -> Variant:
	var p := _data_dir.path_join(file)
	if FileAccess.file_exists(p):
		var d: Variant = JSON.parse_string(FileAccess.get_file_as_string(p))
		if typeof(d) == typeof(empty):
			return d
	return empty


func _save() -> void:
	for pair in [["players.json", _players], ["times.json", _times]]:
		var f := FileAccess.open(_data_dir.path_join(pair[0]) + ".tmp", FileAccess.WRITE)
		if f:
			f.store_string(JSON.stringify(pair[1]))
			f.close()
			DirAccess.rename_absolute(_data_dir.path_join(pair[0]) + ".tmp", _data_dir.path_join(pair[0]))
	_dirty = false


func _process(_delta: float) -> bool:
	while _server.is_connection_available():
		var ws := WebSocketPeer.new()
		ws.inbound_buffer_size = 1 << 20
		ws.outbound_buffer_size = 1 << 20
		var tcp := _server.take_connection()
		var from_ip := tcp.get_connected_host()
		ws.accept_stream(tcp)
		_conns.append({"ws": ws, "code": "", "party": "", "id": 0, "ip": from_ip, "ip_done": false})
	for c in _conns.duplicate():
		var ws: WebSocketPeer = c["ws"]
		ws.poll()
		var st := ws.get_ready_state()
		if st == WebSocketPeer.STATE_OPEN:
			if not bool(c["ip_done"]):
				c["ip_done"] = true
				c["ip"] = Geo.real_ip(ws, str(c["ip"]))
			while ws.get_available_packet_count() > 0:
				_handle(c, ws.get_packet().get_string_from_utf8())
		elif st == WebSocketPeer.STATE_CLOSED:
			_conns.erase(c)
			_leave_party(c)
	var now := Time.get_unix_time_from_system()
	for h in _parties.keys():
		var p: Dictionary = _parties[h]
		if (p["peers"] as Dictionary).is_empty() and now - float(p["empty_since"]) > PARTY_KEEP_SECS:
			_parties.erase(h)
	for k in _invites.keys():
		if now - float(_invites[k]["at"]) > INVITE_SECS * 3:
			_invites.erase(k)
	if _dirty and now > _save_at:
		_save_at = now + 10.0
		_save()
	return false


func _send(c: Dictionary, msg: Dictionary) -> void:
	(c["ws"] as WebSocketPeer).send_text(JSON.stringify(msg))


func _handle(c: Dictionary, text: String) -> void:
	var m: Variant = JSON.parse_string(text)
	if not (m is Dictionary):
		return
	var now := Time.get_unix_time_from_system()
	if str(c["code"]) != "" and _players.has(c["code"]):
		_players[c["code"]]["seen"] = now
	match str(m.get("t", "")):
		"ask":
			_send(c, {"t": "ans", "rid": m.get("rid", 0), "r": _answer(c, m)})
		"host":
			_host(c)
		"join":
			_join(c, str(m.get("host", "")))
		"leave":
			_leave_party(c)
		"world":
			_world_queue(c, bool(m.get("region_ok", false)))
		"world_open":
			if _world.has(str(c["code"])) and int(c["id"]) == 1:
				_world[str(c["code"])]["open"] = bool(m.get("open", true))
		"to":
			var p: Dictionary = _parties.get(str(c["party"]), {})
			var target: Variant = (p.get("peers", {}) as Dictionary).get(int(m.get("to", -1)))
			if target != null:
				_send(target, {"t": "from", "from": int(c["id"]), "data": m.get("data", {})})


# --- World races: strangers in the United States and Canada, matched automatically. No chat. ---

const WORLD_MAX := 12
var _world: Dictionary = {}        ## host player -> {open}: world parties and whether they take new racers


func _world_queue(c: Dictionary, region_ok: bool) -> void:
	# The hard lock: the player's internet address must belong to the United States or Canada
	# (from the internet registries' own lists). Only if those lists couldn't be loaded at all
	# does the server fall back to the game's own time-zone check. The same if the player's real
	# address is hidden (a tunnel or proxy such as Cloudflare in front that doesn't pass it on).
	if _geo.ready() and not Geo.is_local(str(c["ip"])):
		region_ok = _geo.allowed(str(c["ip"]))
	if not region_ok:
		_send(c, {"t": "error", "msg": "World races are for players in the United States and Canada."})
		return
	if str(c["code"]) == "":
		_send(c, {"t": "error", "msg": "Set your name first."})
		return
	_leave_party(c)
	# Fullest open world party first, so races fill up.
	var best := ""
	var best_n := 0
	for h in _world:
		var p: Dictionary = _parties.get(h, {})
		var n := (p.get("peers", {}) as Dictionary).size()
		if bool(_world[h]["open"]) and (p.get("peers", {}) as Dictionary).has(1) and n < WORLD_MAX and n > best_n:
			best = h
			best_n = n
	if best != "":
		_send(c, {"t": "world_join", "host": best})
		return
	# Nobody waiting: this player starts a new world party and others are sent to it.
	_world[str(c["code"])] = {"open": true}
	_send(c, {"t": "world_host"})
	_host(c)


# --- Parties: found by the host's player, never by a code -------------------------------------

func _host(c: Dictionary) -> void:
	var me := str(c["code"])
	if me == "":
		_send(c, {"t": "error", "msg": "Set your name first."})
		return
	_leave_party(c)
	if not _parties.has(me):
		_parties[me] = {"next_id": 2, "peers": {}, "empty_since": 0.0}
	var p: Dictionary = _parties[me]
	var old: Variant = (p["peers"] as Dictionary).get(1)
	if old != null and not is_same(old, c):
		old["party"] = ""   # the same player hosting from a new tab: the new one wins
	p["peers"][1] = c
	c["party"] = me
	c["id"] = 1
	_send(c, {"t": "room", "id": 1})
	# Anyone who waited for the host to come back meets them again.
	for id in p["peers"]:
		if int(id) != 1:
			_send(c, {"t": "peer", "id": int(id)})
			_send(p["peers"][id], {"t": "peer", "id": 1})


func _join(c: Dictionary, host: String) -> void:
	_leave_party(c)
	if not _parties.has(host):
		_send(c, {"t": "error", "msg": "That party has closed."})
		return
	var p: Dictionary = _parties[host]
	if (p["peers"] as Dictionary).size() >= MAX_PER_PARTY:
		_send(c, {"t": "error", "msg": "That party is full (16)."})
		return
	var id := int(p["next_id"])
	p["next_id"] = id + 1
	p["peers"][id] = c
	c["party"] = host
	c["id"] = id
	_send(c, {"t": "room", "id": id})
	if (p["peers"] as Dictionary).has(1):
		_send(c, {"t": "peer", "id": 1})
		_send(p["peers"][1], {"t": "peer", "id": id})


func _leave_party(c: Dictionary) -> void:
	var h := str(c["party"])
	c["party"] = ""
	if not _parties.has(h):
		return
	var p: Dictionary = _parties[h]
	if not is_same(p["peers"].get(int(c["id"])), c):
		return
	(p["peers"] as Dictionary).erase(int(c["id"]))
	if int(c["id"]) == 1:
		_world.erase(h)   # a world party ends when its host leaves; the others get matched again
	for id in p["peers"]:
		_send(p["peers"][id], {"t": "gone", "id": int(c["id"])})
	if (p["peers"] as Dictionary).is_empty():
		p["empty_since"] = Time.get_unix_time_from_system()


# --- Questions: names, online, invites, leaderboards ----------------------------------------

func _online(code: String) -> bool:
	for c in _conns:
		if str(c["code"]) == code:
			return true
	return Time.get_unix_time_from_system() - float((_players.get(code, {}) as Dictionary).get("seen", 0.0)) < ONLINE_SECS


func _name_free(nm: String, code: String) -> bool:
	var low := nm.strip_edges().to_lower()
	for k in _players:
		if k != code and str(_players[k]["name"]).to_lower() == low:
			return false
	return true


func _answer(c: Dictionary, m: Dictionary) -> Variant:
	var now := Time.get_unix_time_from_system()
	match str(m.get("op", "")):
		"hello":
			c["code"] = str(m.get("code", ""))
			if _players.has(c["code"]):
				_players[c["code"]]["seen"] = now
			return true
		"name_free":
			return _name_free(str(m.get("name", "")), str(m.get("code", "")))
		"register":
			var code := str(m.get("code", ""))
			var nm := str(m.get("name", "")).strip_edges()
			if code == "" or nm.length() < 3 or nm.length() > 16 or not _name_free(nm, code):
				return false
			c["code"] = code
			if not _players.has(code) or str(_players[code]["name"]) != nm:
				_dirty = true
			_players[code] = {"name": nm, "seen": now}
			return true
		"find":
			var q := str(m.get("q", "")).strip_edges().to_lower()
			var out: Array = []
			if q.length() >= 2:
				for k in _players:
					if k != str(m.get("code", "")) and str(_players[k]["name"]).to_lower().contains(q):
						out.append({"code": k, "name": _players[k]["name"], "online": _online(k)})
						if out.size() >= 20:
							break
			return out
		"by_codes":
			var out := {}
			for k in m.get("codes", []):
				if _players.has(str(k)):
					out[str(k)] = {"name": _players[str(k)]["name"], "online": _online(str(k))}
			return out
		"invite":
			var rec := {"to_code": str(m.get("to", "")), "from_code": str(m.get("from", "")), "from_name": str(m.get("from_name", "")),
					"party": str(m.get("party", "")), "state": "", "at": now}
			_invites["%s|%s" % [rec["to_code"], rec["from_code"]]] = rec
			return true
		"my_invites":
			var out: Array = []
			for k in _invites:
				var e: Dictionary = _invites[k]
				if str(e["to_code"]) == str(m.get("code", "")) and now - float(e["at"]) < INVITE_SECS and str(e["state"]) in ["", "seen"]:
					out.append({"from_code": e["from_code"], "from_name": e["from_name"], "party": e["party"],
							"state": e["state"], "age": now - float(e["at"])})
			return out
		"mark_invite":
			var k := "%s|%s" % [str(m.get("to", "")), str(m.get("from", ""))]
			if _invites.has(k):
				_invites[k]["state"] = str(m.get("state", ""))
			return true
		"invite_state":
			return str((_invites.get("%s|%s" % [str(m.get("to", "")), str(m.get("from", ""))], {}) as Dictionary).get("state", ""))
		"post_time":
			var t := float(m.get("time", 0.0))
			if t > 0.0 and str(m.get("track_key", "")) != "":
				_times.append({"track_key": str(m["track_key"]), "track_name": str(m.get("track_name", "")).substr(0, 60),
						"player_id": str(m.get("player_id", "")), "player_name": str(m.get("player_name", "")).substr(0, 16),
						"time": snappedf(t, 0.01), "at": now})
				if _times.size() > MAX_TIMES:
					_times = _times.slice(_times.size() - MAX_TIMES)
				_dirty = true
			return true
		"board":
			var key := str(m.get("key", ""))
			var since := float(m.get("since", 0.0))
			var ids: Array = m.get("ids", [])
			var out: Array = []
			for r in _times:
				if str(r["track_key"]) == key and float(r["at"]) >= since and ids.has(str(r["player_id"])):
					out.append(r)
			return out
		"world_board":
			# Everyone's best time on this track in the period: the top 100.
			var key := str(m.get("key", ""))
			var since := float(m.get("since", 0.0))
			var best := {}
			for r in _times:
				if str(r["track_key"]) == key and float(r["at"]) >= since:
					var id := str(r["player_id"])
					if not best.has(id) or float(r["time"]) < float(best[id]["time"]):
						best[id] = r
			var out: Array = best.values()
			out.sort_custom(func(a: Dictionary, b: Dictionary) -> bool: return float(a["time"]) < float(b["time"]))
			return out.slice(0, 100)
		"board_tracks":
			var ids: Array = m.get("ids", [])
			var everyone := bool(m.get("world", false))
			var seen := {}
			var out: Array = []
			for r in _times:
				if (everyone or ids.has(str(r["player_id"]))) and not seen.has(r["track_key"]):
					seen[r["track_key"]] = true
					out.append({"key": str(r["track_key"]), "name": str(r["track_name"])})
			return out
	return null


# --- The US and Canada lock: which internet addresses belong to which country ------------------
# The five internet registries (ARIN, RIPE, APNIC, LACNIC, AFRINIC) publish free lists of every
# block of addresses and the country it was given to. The server downloads them by itself when it
# starts and once a day, keeps only the US and Canada blocks, and saves a copy (geo_us_ca.json)
# so it still works after a restart with no internet. Nobody has to do anything.
#
# Behind a web server (nginx/Apache passing wss://.../marble-server on), every player looks like
# 127.0.0.1, so the web server must pass the real address on: nginx
#   proxy_pass http://127.0.0.1:9080/?ip=$remote_addr;
# The ?ip= is only believed from the server's own machine or home network, never from outside.

class Geo:
	extends Node

	const LISTS := [
		"https://ftp.arin.net/pub/stats/arin/delegated-arin-extended-latest",
		"https://ftp.ripe.net/pub/stats/ripencc/delegated-ripencc-extended-latest",
		"https://ftp.apnic.net/stats/apnic/delegated-apnic-extended-latest",
		"https://ftp.lacnic.net/pub/stats/lacnic/delegated-lacnic-extended-latest",
		"https://ftp.afrinic.net/pub/stats/afrinic/delegated-afrinic-extended-latest",
	]
	const COUNTRIES := ["US", "CA"]
	const REFRESH_SECS := 86400.0

	var _dir := ""
	var _v4: Array = []      ## sorted [start, end] (whole numbers)
	var _v6: Array = []      ## sorted [start, end] of the top 64 bits
	var _new4: Array = []
	var _new6: Array = []
	var _todo: Array = []
	var _got := 0
	var _http: HTTPRequest
	var _next_refresh := 0.0

	func _init(data_dir: String) -> void:
		_dir = data_dir

	func _ready() -> void:
		_http = HTTPRequest.new()
		_http.timeout = 120.0
		_http.body_size_limit = 64 << 20
		add_child(_http)
		_http.request_completed.connect(_on_list)
		var saved: Variant = JSON.parse_string(FileAccess.get_file_as_string(_dir.path_join("geo_us_ca.json"))) \
				if FileAccess.file_exists(_dir.path_join("geo_us_ca.json")) else null
		if saved is Dictionary:
			_v4 = (saved as Dictionary).get("v4", [])
			_v6 = (saved as Dictionary).get("v6", [])
			print("marble server: US/Canada address list loaded from the saved copy (%d blocks)" % (_v4.size() + _v6.size()))
		_refresh()

	func ready() -> bool:
		return not _v4.is_empty()

	func _process(_delta: float) -> void:
		if _todo.is_empty() and Time.get_unix_time_from_system() > _next_refresh:
			_refresh()

	func _refresh() -> void:
		_next_refresh = Time.get_unix_time_from_system() + REFRESH_SECS
		_todo = LISTS.duplicate()
		_new4 = []
		_new6 = []
		_got = 0
		_fetch_next()

	func _fetch_next() -> void:
		if _todo.is_empty():
			if _got == LISTS.size() and not _new4.is_empty():
				_v4 = _merge(_new4)
				_v6 = _merge(_new6)
				var f := FileAccess.open(_dir.path_join("geo_us_ca.json"), FileAccess.WRITE)
				if f:
					f.store_string(JSON.stringify({"v4": _v4, "v6": _v6, "at": Time.get_datetime_string_from_system()}))
					f.close()
				print("marble server: US/Canada address list updated (%d blocks)" % (_v4.size() + _v6.size()))
			elif not ready():
				print("marble server: couldn't download the address lists; trying again in an hour")
				_next_refresh = Time.get_unix_time_from_system() + 3600.0
			return
		if _http.request(str(_todo.pop_front())) != OK:
			_fetch_next()

	func _on_list(result: int, code: int, _headers: PackedStringArray, body: PackedByteArray) -> void:
		if result == HTTPRequest.RESULT_SUCCESS and code == 200:
			_got += 1
			add_lines(body.get_string_from_utf8(), _new4, _new6)
		_fetch_next()

	## Lines look like: arin|US|ipv4|8.0.0.0|8388608|19921201|allocated|...
	static func add_lines(text: String, out4: Array, out6: Array) -> void:
		for line in text.split("\n", false):
			var p := line.split("|")
			if p.size() < 5 or not COUNTRIES.has(p[1]):
				continue
			if p[2] == "ipv4":
				var s := v4_num(p[3])
				if s >= 0:
					out4.append([s, s + int(p[4]) - 1])
			elif p[2] == "ipv6":
				var s6 := v6_top(p[3])
				var bits := clampi(int(p[4]), 1, 64)
				if s6 >= 0:
					out6.append([s6, s6 + (1 << (64 - bits)) - 1 if bits > 1 else 0x7fffffffffffffff])

	static func _merge(r: Array) -> Array:
		r.sort_custom(func(a: Array, b: Array) -> bool: return int(a[0]) < int(b[0]))
		var out: Array = []
		for x in r:
			if not out.is_empty() and int(x[0]) <= int(out[-1][1]) + 1:
				out[-1][1] = maxi(int(out[-1][1]), int(x[1]))
			else:
				out.append([int(x[0]), int(x[1])])
		return out

	static func _inside(r: Array, n: int) -> bool:
		var lo := 0
		var hi := r.size() - 1
		while lo <= hi:
			var mid := (lo + hi) >> 1
			if n < int(r[mid][0]):
				hi = mid - 1
			elif n > int(r[mid][1]):
				lo = mid + 1
			else:
				return true
		return false

	func allowed(ip: String) -> bool:
		if is_local(ip):
			return true   # testing on the server's own machine or home network
		var mapped := ip.trim_prefix("::ffff:")
		if mapped.contains(".") and not mapped.contains(":"):
			return _inside(_v4, v4_num(mapped))
		var t := v6_top(ip)
		return t >= 0 and _inside(_v6, t)

	static func v4_num(s: String) -> int:
		var p := s.split(".")
		if p.size() != 4:
			return -1
		var n := 0
		for x in p:
			if not x.is_valid_int() or int(x) < 0 or int(x) > 255:
				return -1
			n = n * 256 + int(x)
		return n

	## The first 64 bits of an IPv6 address (-1 if it isn't one, or isn't a normal internet address).
	static func v6_top(s: String) -> int:
		s = s.split("%")[0]
		if not s.contains(":"):
			return -1
		var halves := s.split("::")
		if halves.size() > 2:
			return -1
		var head := halves[0].split(":", false)
		var tail := halves[1].split(":", false) if halves.size() == 2 else PackedStringArray()
		var groups: Array = []
		for g in head:
			groups.append(g)
		for i in 8 - head.size() - tail.size():
			groups.append("0")
		for g in tail:
			groups.append(g)
		if groups.size() != 8:
			return -1
		var n := 0
		for i in 4:
			var g := str(groups[i])
			if not g.is_valid_hex_number() or g.length() > 4:
				return -1
			n = (n << 16) | g.hex_to_int()
		return n if n >= 0 else -1

	static func is_local(ip: String) -> bool:
		var m := ip.trim_prefix("::ffff:")
		return m == "127.0.0.1" or m == "::1" or m.begins_with("10.") or m.begins_with("192.168.") \
				or (m.begins_with("172.") and m.split(".").size() == 4 and int(m.split(".")[1]) >= 16 and int(m.split(".")[1]) <= 31) \
				or m.begins_with("fd") or m.begins_with("fc")

	## A player's real address: straight from the connection, or (only when the connection comes
	## from the server's own machine, i.e. through the web server) the ?ip= the web server adds.
	static func real_ip(ws: WebSocketPeer, direct: String) -> String:
		if not is_local(direct) or not ws.has_method("get_requested_url"):
			return direct
		var url := str(ws.call("get_requested_url"))
		var q := url.get_slice("?", 1)
		for part in q.split("&", false):
			if part.begins_with("ip="):
				return part.substr(3).uri_decode().strip_edges()
		return direct


# --- Updating itself from Cody's GitHub (Cody's computer is the master) ------------------------
# Once an hour the server fetches server/marble_server.gd and its signature from the game's
# official address. It only takes a file signed with the key that exists on Cody's computer.
# If the signed file is different from this one, it is saved over this file and the server
# stops; the system starts it again straight away (Restart=always) with the new version.

class SelfUpdate:
	extends Node

	const OFFICIAL := "https://johnsonc689-bot.github.io/marble-racers-days/server/"
	const CHECK_SECS := 3600.0
	const PUBLIC_KEY := """-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAv1ZoZPTSGFHDYBHKUO7i
4hzjjQ6cGHw68e1fMZUpyOz3V1CCYtqDKVHWft42DB8+SZiheMUfzk71RqXFMx1P
i7HncAEP6/Az36yUM+OMtirS+IpdoUcFZTG7inuEJX9K6+fisFXBPg6PrrarpXP3
PltfMEWbBc22tosfpy0ofFUze3rws6lmgzkNdPBzZIH50GcIDW6LhEhDrF7XRJoS
71XLHH108CaSveraE6oqRZTHJOnEfklG2WBmIjGzA/oqwOnqC6HNJGhVEZjPpPl5
acC4pHqs1v43HLiWsMfn025BnlBna/81n1ECam8/QNkO0uQPG+1dM9bisU7JQebo
XwIDAQAB
-----END PUBLIC KEY-----
"""

	var _tree: SceneTree
	var _http: HTTPRequest
	var _sig := ""
	var _next := 0.0

	func _init(tree: SceneTree) -> void:
		_tree = tree

	func _ready() -> void:
		_http = HTTPRequest.new()
		_http.timeout = 60.0
		add_child(_http)
		_next = Time.get_unix_time_from_system() + 30.0

	func _process(_delta: float) -> void:
		if Time.get_unix_time_from_system() < _next:
			return
		_next = Time.get_unix_time_from_system() + CHECK_SECS
		_fetch("marble_server.sig", _on_sig)

	func _fetch(file: String, cb: Callable) -> void:
		if _http.get_http_client_status() != HTTPClient.STATUS_DISCONNECTED:
			return
		_http.request_completed.connect(cb, CONNECT_ONE_SHOT)
		if _http.request(OFFICIAL + file + "?t=%d" % int(Time.get_unix_time_from_system())) != OK:
			_http.request_completed.disconnect(cb)

	func _on_sig(result: int, code: int, _h: PackedStringArray, body: PackedByteArray) -> void:
		if result != HTTPRequest.RESULT_SUCCESS or code != 200:
			return
		_sig = body.get_string_from_utf8().strip_edges()
		_fetch.call_deferred("marble_server.gd", _on_file)

	static func signed_ok(data: PackedByteArray, sig_b64: String) -> bool:
		var key := CryptoKey.new()
		if sig_b64 == "" or key.load_from_string(PUBLIC_KEY, true) != OK:
			return false
		var hc := HashingContext.new()
		hc.start(HashingContext.HASH_SHA256)
		hc.update(data)
		return Crypto.new().verify(HashingContext.HASH_SHA256, hc.finish(), Marshalls.base64_to_raw(sig_b64), key)

	func _on_file(result: int, code: int, _h: PackedStringArray, body: PackedByteArray) -> void:
		if result != HTTPRequest.RESULT_SUCCESS or code != 200 or body.is_empty():
			return
		if not signed_ok(body, _sig):
			print("marble server: an update was found but it isn't signed by Cody; ignored")
			return
		var me: String = (_tree.get_script() as Script).resource_path
		var path := ProjectSettings.globalize_path(me) if me.begins_with("res://") or me.begins_with("user://") else me
		if FileAccess.get_file_as_bytes(path) == body:
			return   # already the newest
		var f := FileAccess.open(path + ".new", FileAccess.WRITE)
		if f == null:
			print("marble server: can't write the update next to %s" % path)
			return
		f.store_buffer(body)
		f.close()
		if DirAccess.rename_absolute(path + ".new", path) != OK:
			print("marble server: can't replace %s" % path)
			return
		print("marble server: updated from Cody's GitHub; restarting")
		_tree.quit(0)
