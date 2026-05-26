--// @chi0sk / sam

local HttpService = game:GetService("HttpService")

export type EventName = string
export type HandlerFn = (payload: any, meta: EventMeta) -> ()

export type EventMeta = {
	Event: EventName,
	Time: number,
	Server: string,
	IsRemote: boolean,
}

export type SubscribeOptions = {
	Priority: number?,
	Once: boolean?,
	Filter: ((payload: any) -> boolean)?,
}

export type Config = {
	MaxDepth: number?,
	OnError: ((err: string) -> ())?,
}

export type Metrics = {
	TotalEmitted: number,
	TotalDispatched: number,
	TotalBlocked: number,
	Subscribers: number,
}

export type Namespace = {
	On: (self: Namespace, event: EventName, handler: HandlerFn, opts: SubscribeOptions?) -> () -> (),
	Once: (self: Namespace, event: EventName, handler: HandlerFn, opts: SubscribeOptions?) -> () -> (),
	Emit: (self: Namespace, event: EventName, payload: any?) -> (),
	EmitDeferred: (self: Namespace, event: EventName, payload: any?) -> (),
	Namespace: (self: Namespace, prefix: string) -> Namespace,
}

type Subscriber = {
	id: string,
	handler: HandlerFn,
	priority: number,
	once: boolean,
	filter: ((payload: any) -> boolean)?,
}

local DEFAULT_MAX_DEPTH = 8
local SERVER_ID = if game.JobId ~= "" then tostring(game.JobId) else "studio_" .. HttpService:GenerateGUID(false)

local function clone(t: any): any
	if type(t) ~= "table" then
		return t
	end

	local copy = {}
	for k, v in pairs(t) do
		copy[k] = clone(v)
	end

	return copy
end

local eventbus = {}
eventbus.__index = eventbus

function eventbus.new(config: Config?)
	config = config or {}

	local self = setmetatable({}, eventbus)

	self._config = config
	self._events = {} :: {[string]: {Subscriber}}
	self._max_depth = (config :: any).MaxDepth or DEFAULT_MAX_DEPTH
	self._depth = 0
	self._destroyed = false

	self._metrics = {
		TotalEmitted = 0,
		TotalDispatched = 0,
		TotalBlocked = 0,
		Subscribers = 0,
	} :: Metrics

	return self
end

function eventbus:On(event: EventName, handler: HandlerFn, opts: SubscribeOptions?): () -> ()
	assert(not self._destroyed, "eventbus is destroyed")
	assert(type(event) == "string" and #event > 0, "event must be a non-empty string")
	assert(type(handler) == "function", "handler must be a function")

	opts = opts or {}

	local list = self._events[event]
	if list == nil then
		list = {}
		self._events[event] = list
	end

	local sub: Subscriber = {
		id = HttpService:GenerateGUID(false),
		handler = handler,
		priority = (opts :: any).Priority or 0,
		once = (opts :: any).Once == true,
		filter = (opts :: any).Filter,
	}

	table.insert(list, sub)
	table.sort(list, function(a, b)
		return a.priority > b.priority
	end)

	self._metrics.Subscribers += 1

	return function()
		local current = self._events[event]
		if current == nil then
			return
		end

		for i, item in ipairs(current) do
			if item.id == sub.id then
				table.remove(current, i)
				self._metrics.Subscribers -= 1
				return
			end
		end
	end
end

function eventbus:Once(event: EventName, handler: HandlerFn, opts: SubscribeOptions?): () -> ()
	local nextOpts = table.clone(opts or {})
	nextOpts.Once = true

	return self:On(event, handler, nextOpts)
end

function eventbus:Emit(event: EventName, payload: any?)
	assert(not self._destroyed, "eventbus is destroyed")
	assert(type(event) == "string" and #event > 0, "event must be a non-empty string")

	self:_emit(event, payload, false, nil)
end

function eventbus:EmitDeferred(event: EventName, payload: any?)
	task.defer(function()
		if not self._destroyed then
			self:Emit(event, payload)
		end
	end)
end

function eventbus:SubscriberCount(event: EventName): number
	local list = self._events[event]
	return if list then #list else 0
end

function eventbus:RemoveAll(event: EventName)
	local list = self._events[event]
	if list == nil then
		return
	end

	self._metrics.Subscribers -= #list
	self._events[event] = nil
end

function eventbus:GetMetrics(): Metrics
	return clone(self._metrics)
end

function eventbus:Namespace(prefix: string): Namespace
	assert(type(prefix) == "string" and #prefix > 0, "prefix must be a non-empty string")

	local root = self
	local ns = {} :: Namespace

	function ns:On(event, handler, opts)
		return root:On(prefix .. "." .. event, handler, opts)
	end

	function ns:Once(event, handler, opts)
		return root:Once(prefix .. "." .. event, handler, opts)
	end

	function ns:Emit(event, payload)
		root:Emit(prefix .. "." .. event, payload)
	end

	function ns:EmitDeferred(event, payload)
		root:EmitDeferred(prefix .. "." .. event, payload)
	end

	function ns:Namespace(sub)
		return root:Namespace(prefix .. "." .. sub)
	end

	return ns
end

function eventbus:Destroy()
	self._destroyed = true
	self._events = {}
	self._metrics.Subscribers = 0
end

function eventbus:_emit(event: EventName, payload: any, isRemote: boolean, originServer: string?)
	if self._depth >= self._max_depth then
		self:_onError("too many event calls: " .. event)
		return
	end

	self._metrics.TotalEmitted += 1
	self._depth += 1

	local list = self._events[event]
	if list == nil or #list == 0 then
		self._metrics.TotalBlocked += 1
		self._depth -= 1
		return
	end

	local meta: EventMeta = {
		Event = event,
		Time = os.time(),
		Server = originServer or SERVER_ID,
		IsRemote = isRemote,
	}

	local remove = {}
	local dispatched = 0

	for _, sub in ipairs(table.clone(list)) do
		if sub.filter then
			local ok, allowed = pcall(sub.filter, payload)
			if not ok or not allowed then
				continue
			end
		end

		local ok, err = pcall(sub.handler, payload, meta)
		if not ok then
			self:_onError("event failed: " .. event .. " / " .. tostring(err))
		end

		dispatched += 1

		if sub.once then
			table.insert(remove, sub.id)
		end
	end

	for _, id in ipairs(remove) do
		self:_remove(event, id)
	end

	self._metrics.TotalDispatched += dispatched
	self._depth -= 1
end

function eventbus:_remove(event: EventName, id: string)
	local list = self._events[event]
	if list == nil then
		return
	end

	for i = #list, 1, -1 do
		if list[i].id == id then
			table.remove(list, i)
			self._metrics.Subscribers -= 1
			return
		end
	end
end

function eventbus:_onError(err: string)
	local onError = self._config and (self._config :: any).OnError
	if onError then
		task.spawn(onError, err)
	else
		warn("[eventbus] " .. err)
	end
end

return eventbus
