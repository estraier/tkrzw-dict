local r = lighty.r

local COOKIE_NAME = "dbmx_captcha"
local COOKIE_MAX_AGE = 31 * 24 * 60 * 60

local client_address =
    r.req_header["X-Forwarded-For"]
    or r.req_attr["request.remote-addr"]
    or ""

client_address = client_address:match("^%s*(.-)%s*$")

local month = os.date("%Y%m")
local digest = lighty.c.md("sha256", client_address .. month)

-- Equivalent to:
-- int.from_bytes(digest[:8], "big") % 1000000
--
-- lighty.c.md() returns hexadecimal, so calculate the modulo
-- incrementally without converting the whole 64-bit value to Lua number.
local value = 0
for i = 1, 16 do
    local c = digest:byte(i)
    local digit
    if c >= 48 and c <= 57 then
        digit = c - 48
    elseif c >= 65 and c <= 70 then
        digit = c - 65 + 10
    else
        return 500
    end
    value = (value * 16 + digit) % 1000000
end

local token = string.format("%06d", value)

local cookie_value = nil
local cookie_header = r.req_header["Cookie"] or ""

for part in cookie_header:gmatch("([^;]+)") do
    local name, val = part:match("^%s*([^=]+)=(.*)$")
    if name == COOKIE_NAME then
        cookie_value = val
        break
    end
end

if cookie_value ~= nil
    and lighty.c.secret_eq(cookie_value, token) then
    return 0
end

local body = string.format([[
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="robots" content="noindex,nofollow,noarchive"/>
<meta name="description" content="Open-source dicitionary made from WordNet, Wiktionary, refined by AI models"/>
<title>English-Japanese Dictionary</title>
<style>
body { margin: 2em; ont-family: sans-serif; }
button { padding: 0.8em 1.2em; font-size: 1rem; cursor: pointer; }
</style>
</head>
<body>
<p>This service uses a simple check to prevent automated access.<br/>
Please click the button below to continue.</p>
<button type="button"
 onclick="document.cookie='%s=%s; Max-Age=%d; Path=/; SameSite=None; Secure'; location.reload();">I'm not a robot.</button>
</body>
</html>
]], COOKIE_NAME, token, COOKIE_MAX_AGE)

r.resp_header["Content-Type"] = "text/html; charset=UTF-8"
r.resp_header["Cache-Control"] = "no-store"
r.resp_body:set({body})

return 200
