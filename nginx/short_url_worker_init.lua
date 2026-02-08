function read_short_url_version(type)
    local version_file = get_version_file(type)
    if version_file then
        local version_data = version_file:read("*a")
        version_data = version_data:gsub("^%s*(.-)%s*$", "%1")
        version_file:close()
        print(type .. " dict version: " .. version_data)
        return version_data
    end
    return nil
end

function get_version_file(type)
    if type == "deeplink" then
        local deeplink_file = io.open("conf/deeplink.version", "r")
        return deeplink_file
    else
        local version_file = io.open("conf/short_url.version", "r")
        return version_file
    end
end

function get_cache_version(short_url_dict, type)
    if type == "deeplink" then
        local deeplink_version = short_url_dict:get("DEEPLINK_VERSION")
        return deeplink_version
    else
        local current_version = short_url_dict:get("VERSION")
        return current_version
    end
end

function read_redirect_url()
    local redirect_file = io.open("conf/short_url_redirect.default", "r")
    if redirect_file then
        local file_data = redirect_file:read("*a")
        local url_data = file_data:gsub("^%s*(.-)%s*$", "%1")
        local success, error = ngx.shared.short_url:set("redirect_url", url_data)
        if not success then
            ngx.log(ngx.ERR, "Error is :", error);
        end
        redirect_file:close()
    end
end

function get_dict_cache(type)
    if type == "deeplink" then
        local deeplink_dict = ngx.shared.deeplink
        return deeplink_dict
    else
        local short_url_dict = ngx.shared.short_url
        return short_url_dict
    end
end

function read_short_url_dict(type)
    local dict_version = read_short_url_version(type)
    if dict_version then
        local short_url_dict = get_dict_cache(type)
        if short_url_dict then
            local current_version = get_cache_version(short_url_dict, type)
            if current_version == dict_version then
                print(type .. " dict do not need to update")
                return
            end

            local dict_file = io.open(dict_version, "r")
            if dict_file then
                local read_line = 0
                local new_line = 0
                for line in dict_file:lines() do
                    read_line = read_line + 1
                    local _, _, k, v = string.find(line, "(.+) (.+)")
                    if short_url_dict:get(k) then
                        break
                    end
                    local add_success, err = short_url_dict:add(k, v) -- add 本身具有 LRU 的功能
                    if add_success then
                        new_line = new_line + 1
                    else
                        ngx.log(ngx.ERR, "ERROR :" .. err .. " . The key is " .. k)
                    end
                end
                ngx.log(ngx.ERR, type .. " dict read line: " .. read_line .. ", new line: " .. new_line)
                if type == "deeplink" then
                    short_url_dict:set("DEEPLINK_VERSION", dict_version)
                else
                    short_url_dict:set("VERSION", dict_version)
                end
                dict_file:close()
                return new_line
            else
                ngx.log(ngx.ERR, "can't find dict file!! filename: " .. dict_version)
            end
        end
    end
    return 0
end

function load_white_url_list()
    -- 初始化渠道跳转的白名单
    ngx.shared.channel_redirect_url_white_list:flush_all()
    -- flush all 只是标记未过期，flush expired 才会真正删除
    ngx.shared.channel_redirect_url_white_list:flush_expired()
    local white_list_file = io.open("channel/channel_redirect_white_list", "r")
    local failed_reason = nil
    if white_list_file then
        for line in white_list_file:lines() do
            local ok, err
            if line == "enable_sensors_channel_white_list" then
                ok, err = ngx.shared.channel_redirect_url_white_list:safe_add("enable_sensors_channel_white_list", "TRUE")
            else
                ok, err = ngx.shared.channel_redirect_url_white_list:safe_add(line, "F")
            end
            -- 忽略掉已存在的错误
            if not ok and err ~= "exists" then
                ngx.log(ngx.ERR, "failed to load white list. reason:" .. err .. ",url=" .. line)
                failed_reason = err
                break
            end
        end
        if failed_reason then
            ngx.log(ngx.ERR, "failed to load white list for channel. reason:" .. failed_reason)
        else
            ngx.log(ngx.ERR, "succeed load white list for channel")
        end
    end
end

local delay = 120
local handler
handler = function(premature)
    if premature then
        return
    end

    read_short_url_dict("short_url")
    read_redirect_url()
    read_short_url_dict("deeplink")

    local ok, err = ngx.timer.at(delay, handler)
    if not ok then
        ngx.log(ngx.ERR, "failed to create short url dict update timer: ", err)
        return
    end
end

if ngx.worker.id() == 0 then
    read_short_url_dict("short_url")
    read_short_url_dict("deeplink")
    read_redirect_url()
    load_white_url_list()
    local ok, err = ngx.timer.at(delay, handler)
    if not ok then
        ngx.log(ngx.ERR, "failed to create short url dict update timer: ", err)
    -- else
        -- ngx.log(ngx.ERR, "start short url dict update timer")
    end
end
