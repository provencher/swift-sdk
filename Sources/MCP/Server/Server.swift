import Logging

import struct Foundation.Data
import struct Foundation.Date
import class Foundation.JSONDecoder
import class Foundation.JSONEncoder
import protocol Foundation.LocalizedError

private enum ResponseSendOutcome: Sendable {
    case sent
    case failed(MCPError)
    case deadlineExceeded
}

private actor ResponseSendRace {
    private var outcome: ResponseSendOutcome?
    private var waiter: CheckedContinuation<ResponseSendOutcome, Never>?

    func resolve(_ outcome: ResponseSendOutcome) {
        guard self.outcome == nil else { return }
        self.outcome = outcome
        waiter?.resume(returning: outcome)
        waiter = nil
    }

    func wait() async -> ResponseSendOutcome {
        if let outcome {
            return outcome
        }
        return await withCheckedContinuation { continuation in
            waiter = continuation
        }
    }
}

/// Model Context Protocol server
public actor Server {
    /// The server configuration
    public struct Configuration: Hashable, Codable, Sendable {
        /// The default configuration.
        public static let `default` = Configuration(strict: false)

        /// The strict configuration.
        public static let strict = Configuration(strict: true)

        /// The maximum time allowed for delivering an encoded JSON-RPC response.
        ///
        /// A deadline expiry is connection-terminal because response delivery state is uncertain.
        public var responseSendTimeout: Duration

        /// When strict mode is enabled, the server:
        /// - Requires clients to send an initialize request before any other requests
        /// - Rejects all requests from uninitialized clients with a protocol error
        ///
        /// While the MCP specification requires clients to initialize the connection
        /// before sending other requests, some implementations may not follow this.
        /// Disabling strict mode allows the server to be more lenient with non-compliant
        /// clients, though this may lead to undefined behavior.
        public var strict: Bool

        public init(
            strict: Bool = false,
            responseSendTimeout: Duration = .seconds(30)
        ) {
            self.strict = strict
            self.responseSendTimeout = responseSendTimeout
        }

        private enum CodingKeys: String, CodingKey {
            case strict
            case responseSendTimeout
        }

        public init(from decoder: Decoder) throws {
            let container = try decoder.container(keyedBy: CodingKeys.self)
            strict = try container.decodeIfPresent(Bool.self, forKey: .strict) ?? false
            responseSendTimeout =
                try container.decodeIfPresent(Duration.self, forKey: .responseSendTimeout)
                ?? .seconds(30)
        }

        public func encode(to encoder: Encoder) throws {
            var container = encoder.container(keyedBy: CodingKeys.self)
            try container.encode(strict, forKey: .strict)
            try container.encode(responseSendTimeout, forKey: .responseSendTimeout)
        }
    }

    /// Implementation information
    public struct Info: Hashable, Codable, Sendable {
        /// The server name
        public let name: String
        /// A human-readable server title for display
        public let title: String?
        /// The server version
        public let version: String
        /// Optional description of the server
        public let description: String?
        /// Optional website URL for the server
        public let websiteUrl: String?
        /// Optional set of sized icons for display in a user interface
        public let icons: [Icon]?

        public init(
            name: String,
            version: String,
            title: String? = nil,
            description: String? = nil,
            websiteUrl: String? = nil,
            icons: [Icon]? = nil
        ) {
            self.name = name
            self.title = title
            self.version = version
            self.description = description
            self.websiteUrl = websiteUrl
            self.icons = icons
        }
    }

    /// Server capabilities
    public struct Capabilities: Hashable, Codable, Sendable {
        /// Resources capabilities
        public struct Resources: Hashable, Codable, Sendable {
            /// Whether the resource can be subscribed to
            public var subscribe: Bool?
            /// Whether the list of resources has changed
            public var listChanged: Bool?

            public init(
                subscribe: Bool? = nil,
                listChanged: Bool? = nil
            ) {
                self.subscribe = subscribe
                self.listChanged = listChanged
            }
        }

        /// Tools capabilities
        public struct Tools: Hashable, Codable, Sendable {
            /// Whether the server notifies clients when tools change
            public var listChanged: Bool?

            public init(listChanged: Bool? = nil) {
                self.listChanged = listChanged
            }
        }

        /// Prompts capabilities
        public struct Prompts: Hashable, Codable, Sendable {
            /// Whether the server notifies clients when prompts change
            public var listChanged: Bool?

            public init(listChanged: Bool? = nil) {
                self.listChanged = listChanged
            }
        }

        /// Logging capabilities
        public struct Logging: Hashable, Codable, Sendable {
            public init() {}
        }

        /// Completions capabilities
        public struct Completions: Hashable, Codable, Sendable {
            public init() {}
        }

        /// Completions capabilities
        public var completions: Completions?
        /// Logging capabilities
        public var logging: Logging?
        /// Prompts capabilities
        public var prompts: Prompts?
        /// Resources capabilities
        public var resources: Resources?
        /// Tools capabilities
        public var tools: Tools?

        public init(
            completions: Completions? = nil,
            logging: Logging? = nil,
            prompts: Prompts? = nil,
            resources: Resources? = nil,
            tools: Tools? = nil
        ) {
            self.completions = completions
            self.logging = logging
            self.prompts = prompts
            self.resources = resources
            self.tools = tools
        }
    }

    /// Server information
    private let serverInfo: Server.Info
    /// The server connection
    private var connection: (any Transport)?
    /// The server logger
    private var logger: Logger? {
        get async {
            await connection?.logger
        }
    }

    /// The server name
    public nonisolated var name: String { serverInfo.name }
    /// A human-readable server title
    public nonisolated var title: String? { serverInfo.title }
    /// The server version
    public nonisolated var version: String { serverInfo.version }
    /// Instructions describing how to use the server and its features
    ///
    /// This can be used by clients to improve the LLM's understanding of
    /// available tools, resources, etc.
    /// It can be thought of like a "hint" to the model.
    /// For example, this information MAY be added to the system prompt.
    public nonisolated let instructions: String?
    /// The server capabilities
    public var capabilities: Capabilities
    /// The server configuration
    public var configuration: Configuration

    private var responseSendDeadlineSleep: @Sendable (Duration) async throws -> Void = {
        duration in
        try await Task.sleep(for: duration)
    }

    /// Request handlers
    private var methodHandlers: [String: RequestHandlerBox] = [:]
    /// Notification handlers
    private var notificationHandlers: [String: [NotificationHandlerBox]] = [:]
    /// Pending request tasks (for cancellation support)
    private var pendingRequestTasks: [ID: Task<Response<AnyMethod>, Error>] = [:]

    /// Pending requests sent to the client, awaiting responses
    private var pendingRequests: [ID: AnyPendingRequest] = [:]
    /// Terminal flag set after response delivery fails or stop begins.
    private var isClosing = false
    /// Shared teardown task so concurrent terminal callers wait for the same disconnect.
    private var connectionCloseTask: Task<Void, Never>?

    private struct ResponseSendContext: Sendable {
        let phase: String
        let requestID: ID?
        let method: String?
        let toolName: String?
        let responseCount: Int
        let startedAt: Date
    }

    /// Whether the server is initialized
    private var isInitialized = false
    /// The client information
    private var clientInfo: Client.Info?
    /// The client capabilities
    private var clientCapabilities: Client.Capabilities?
    /// The protocol version
    private var protocolVersion: String?
    /// The list of subscriptions
    private var subscriptions: [String: Set<ID>] = [:]
    /// The task for the message handling loop
    private var task: Task<Void, Never>?
    /// Request dispatch tasks that have been queued for this connection.
    private var requestDispatchTasks: [ID: Task<Void, Never>] = [:]
    /// Generation token used to reject stale dispatch tasks after terminal close.
    private var connectionGeneration: UInt64 = 0

    public init(
        name: String,
        version: String,
        title: String? = nil,
        instructions: String? = nil,
        capabilities: Server.Capabilities = .init(),
        configuration: Configuration = .default
    ) {
        self.serverInfo = Server.Info(name: name, version: version, title: title)
        self.capabilities = capabilities
        self.configuration = configuration
        self.instructions = instructions
    }

    /// Start the server
    /// - Parameters:
    ///   - transport: The transport to use for the server
    ///   - initializeHook: An optional hook that runs when the client sends an initialize request
    public func start(
        transport: any Transport,
        initializeHook: (@Sendable (Client.Info, Client.Capabilities) async throws -> Void)? = nil
    ) async throws {
        if let connectionCloseTask {
            await connectionCloseTask.value
            self.connectionCloseTask = nil
        }

        self.connection = transport
        self.isClosing = false
        self.connectionGeneration &+= 1
        registerDefaultHandlers(initializeHook: initializeHook)
        registerCancellationHandler()
        try await transport.connect()

        await logger?.debug(
            "Server started", metadata: ["name": "\(name)", "version": "\(version)"]
        )

        // Start message handling loop
        task = Task {
            do {
                let stream = await transport.receive()
                messageLoop: for try await data in stream {
                    if Task.isCancelled || self.isClosing { break }  // Check cancellation inside loop

                    let startedAt = Date()
                    var requestID: ID?
                    do {
                        // Attempt to decode as batch first, then as individual response, request, or notification
                        let decoder = JSONDecoder()
                        if let batch = try? decoder.decode(Server.Batch.self, from: data) {
                            let shouldContinue = await handleBatch(batch, startedAt: startedAt)
                            if !shouldContinue { break messageLoop }
                        } else if let response = try? decoder.decode(AnyResponse.self, from: data) {
                            await handleResponse(response)
                        } else if let request = try? decoder.decode(AnyRequest.self, from: data) {
                            // Queue request handling so already-buffered requests cannot start
                            // after an earlier response delivery failure closes this connection.
                            enqueueRequest(
                                request,
                                startedAt: startedAt,
                                generation: connectionGeneration
                            )
                            try? await Task.sleep(for: .milliseconds(2))
                            if isClosing { break messageLoop }
                        } else if let message = try? decoder.decode(AnyMessage.self, from: data) {
                            try await handleMessage(message)
                        } else {
                            // Try to extract request ID from raw JSON if possible
                            if let json = try? JSONDecoder().decode(
                                [String: Value].self, from: data),
                                let idValue = json["id"]
                            {
                                if let strValue = idValue.stringValue {
                                    requestID = .string(strValue)
                                } else if let intValue = idValue.intValue {
                                    requestID = .number(intValue)
                                }
                            }
                            throw MCPError.parseError("Invalid message format")
                        }
                    } catch let error where MCPError.isResourceTemporarilyUnavailable(error) {
                        // Resource temporarily unavailable, retry after a short delay
                        try? await Task.sleep(for: .milliseconds(10))
                        continue
                    } catch {
                        await logger?.error(
                            "Error processing message", metadata: ["error": "\(error)"])
                        let response = AnyMethod.response(
                            id: requestID ?? .random,
                            error: error as? MCPError
                                ?? MCPError.internalError(error.localizedDescription)
                        )
                        let sent = await sendResponse(
                            response,
                            context: ResponseSendContext(
                                phase: "parse_error_response",
                                requestID: requestID,
                                method: nil,
                                toolName: nil,
                                responseCount: 1,
                                startedAt: startedAt
                            )
                        )
                        if !sent { break messageLoop }
                    }
                }
                await closeConnectionAfterTerminalEvent(throwing: MCPError.connectionClosed)
            } catch {
                await logger?.error(
                    "Fatal error in message handling loop", metadata: ["error": "\(error)"])
                await closeConnectionAfterTerminalEvent(
                    throwing: error as? MCPError ?? MCPError.transportError(error)
                )
            }
            await logger?.debug("Server finished", metadata: [:])
        }
    }

    /// Stop the server
    public func stop() async {
        task?.cancel()
        task = nil
        await closeConnectionAfterTerminalEvent(throwing: MCPError.connectionClosed)
    }

    public func waitUntilCompleted() async {
        await task?.value
    }

    func setResponseSendDeadlineSleepForTesting(
        _ sleep: @escaping @Sendable (Duration) async throws -> Void
    ) {
        responseSendDeadlineSleep = sleep
    }

    // MARK: - Request Context

    /// The JSON-RPC request ID of the currently executing method handler.
    ///
    /// Set via `@TaskLocal` before dispatching each request, so it propagates
    /// automatically into the handler task. Accessible package-wide for
    /// transports that need to identify the active request (e.g. closing an
    /// SSE stream mid-call for reconnection testing per SEP-1699).
    @TaskLocal package static var currentRequestID: ID? = nil

    // MARK: - Registration

    /// Register a method handler
    @discardableResult
    public func withMethodHandler<M: Method>(
        _ type: M.Type,
        handler: @escaping @Sendable (M.Parameters) async throws -> M.Result
    ) -> Self {
        methodHandlers[M.name] = TypedRequestHandler { (request: Request<M>) -> Response<M> in
            let result = try await handler(request.params)
            return Response(id: request.id, result: result)
        }
        return self
    }

    /// Register a notification handler
    @discardableResult
    public func onNotification<N: Notification>(
        _ type: N.Type,
        handler: @escaping @Sendable (Message<N>) async throws -> Void
    ) -> Self {
        let handlers = notificationHandlers[N.name, default: []]
        notificationHandlers[N.name] = handlers + [TypedNotificationHandler(handler)]
        return self
    }

    // MARK: - Sending

    /// Send a response to a request
    public func send<M: Method>(_ response: Response<M>) async throws {
        let startedAt = Date()
        let context = ResponseSendContext(
            phase: "public_response",
            requestID: response.id,
            method: nil,
            toolName: nil,
            responseCount: 1,
            startedAt: startedAt
        )

        let responseData: Data
        do {
            let encoder = JSONEncoder()
            encoder.outputFormatting = [.sortedKeys, .withoutEscapingSlashes]
            responseData = try encoder.encode(response)
        } catch {
            await handleResponseSendFailure(
                error,
                provenance: "response_encode_failed",
                context: context,
                byteCount: nil,
                sendStartedAt: startedAt
            )
            throw MCPError.connectionClosed
        }

        let sent = await sendResponseData(responseData, context: context)
        if !sent {
            throw MCPError.connectionClosed
        }
    }

    private func sendResponse(
        _ response: Response<AnyMethod>,
        context: ResponseSendContext
    ) async -> Bool {
        let responseData: Data
        do {
            let encoder = JSONEncoder()
            encoder.outputFormatting = [.sortedKeys, .withoutEscapingSlashes]
            responseData = try encoder.encode(response)
        } catch {
            await handleResponseSendFailure(
                error,
                provenance: "response_send_failed",
                context: context,
                byteCount: nil,
                sendStartedAt: context.startedAt
            )
            return false
        }

        return await sendResponseData(responseData, context: context)
    }

    private func sendResponseData(
        _ responseData: Data,
        context: ResponseSendContext
    ) async -> Bool {
        guard !isClosing else {
            return false
        }
        guard let connection else {
            await handleResponseSendFailure(
                MCPError.connectionClosed,
                provenance: "response_send_failed",
                context: context,
                byteCount: responseData.count,
                sendStartedAt: Date()
            )
            return false
        }

        let sendStartedAt = Date()
        let race = ResponseSendRace()
        let timeout = configuration.responseSendTimeout
        let deadlineSleep = responseSendDeadlineSleep

        let sendTask = Task {
            do {
                try await connection.send(responseData)
                await race.resolve(.sent)
            } catch let error as MCPError {
                await race.resolve(.failed(error))
            } catch {
                await race.resolve(.failed(MCPError.transportError(error)))
            }
        }
        let deadlineTask = Task {
            do {
                try await deadlineSleep(timeout)
                await race.resolve(.deadlineExceeded)
            } catch {
                // Cancellation means response delivery completed before the deadline.
            }
        }

        let outcome = await race.wait()
        sendTask.cancel()
        deadlineTask.cancel()

        switch outcome {
        case .sent:
            return true
        case .failed(let error):
            await handleResponseSendFailure(
                error,
                provenance: "response_send_failed",
                context: context,
                byteCount: responseData.count,
                sendStartedAt: sendStartedAt
            )
        case .deadlineExceeded:
            await handleResponseSendFailure(
                MCPError.transportError(ResponseSendDeadlineExceeded(timeout: timeout)),
                provenance: "response_send_deadline_exceeded",
                context: context,
                byteCount: responseData.count,
                sendStartedAt: sendStartedAt,
                timeout: timeout
            )
        }
        return false
    }

    private func handleResponseSendFailure(
        _ error: Swift.Error,
        provenance: String,
        context: ResponseSendContext,
        byteCount: Int?,
        sendStartedAt: Date,
        timeout: Duration? = nil
    ) async {
        var metadata: Logger.Metadata = [
            "provenance": "\(provenance)",
            "phase": "\(context.phase)",
            "response_count": "\(context.responseCount)",
            "elapsed_seconds": "\(Date().timeIntervalSince(sendStartedAt))",
            "error_type": "\(String(reflecting: type(of: error)))",
        ]
        if let mcpError = error as? MCPError {
            metadata["error_code"] = "\(mcpError.code)"
        }
        if let requestID = context.requestID {
            metadata["id"] = "\(requestID)"
        }
        if let method = context.method {
            metadata["method"] = "\(method)"
        }
        if let toolName = context.toolName {
            metadata["tool"] = "\(toolName)"
        }
        if let byteCount = byteCount {
            metadata["bytes"] = "\(byteCount)"
        }
        if let timeout {
            metadata["timeout_seconds"] = "\(Self.seconds(in: timeout))"
        }

        await logger?.error("Failed to send JSON-RPC response", metadata: metadata)

        await closeConnectionAfterTerminalEvent(throwing: MCPError.connectionClosed)
    }

    private static func seconds(in duration: Duration) -> Double {
        let components = duration.components
        return Double(components.seconds) + Double(components.attoseconds) / 1e18
    }

    private struct ResponseSendDeadlineExceeded: LocalizedError, Sendable {
        let timeout: Duration

        var errorDescription: String? {
            "JSON-RPC response send exceeded \(Server.seconds(in: timeout)) seconds"
        }
    }

    private func toolName(from request: Request<AnyMethod>) -> String? {
        guard request.method == CallTool.name,
            case .object(let params) = request.params
        else { return nil }
        return params["name"]?.stringValue
    }

    private func enqueueRequest(
        _ request: Request<AnyMethod>,
        startedAt: Date,
        generation: UInt64
    ) {
        let task = Task {
            if Task.isCancelled {
                self.removeDispatchedRequest(id: request.id)
                return
            }
            guard self.canDispatchRequest(generation: generation) else {
                self.removeDispatchedRequest(id: request.id)
                return
            }
            _ = await self.handleRequest(
                request,
                sendResponse: true,
                startedAt: startedAt
            )
            self.removeDispatchedRequest(id: request.id)
        }
        requestDispatchTasks[request.id] = task
    }

    private func canDispatchRequest(generation: UInt64) -> Bool {
        return !isClosing && generation == connectionGeneration
    }

    private func removeDispatchedRequest(id: ID) {
        requestDispatchTasks[id] = nil
    }

    private func cancelDispatchedRequests() {
        let tasks = requestDispatchTasks.values
        requestDispatchTasks = [:]
        for task in tasks {
            task.cancel()
        }
    }

    /// Send a notification to connected clients
    public func notify<N: Notification>(_ notification: Message<N>) async throws {
        if isClosing {
            throw MCPError.connectionClosed
        }
        guard let connection = connection else {
            throw MCPError.internalError("Server connection not initialized")
        }

        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys, .withoutEscapingSlashes]

        let notificationData = try encoder.encode(notification)
        try await connection.send(notificationData)
    }

    /// Send a request to the client and return a Task for the response
    private func send<M: Method>(_ request: Request<M>) throws -> Task<M.Result, Error> {
        if isClosing {
            throw MCPError.connectionClosed
        }
        guard let connection = connection else {
            throw MCPError.internalError("Server connection not initialized")
        }

        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys, .withoutEscapingSlashes]
        let requestData = try encoder.encode(request)

        let requestTask = Task<M.Result, Error> {
            try await withCheckedThrowingContinuation { continuation in
                Task {
                    // Add pending response before sending
                    if let terminalError = self.addPendingResponse(
                        id: request.id,
                        continuation: continuation,
                        type: M.Result.self
                    ) {
                        continuation.resume(throwing: terminalError)
                        return
                    }

                    // Send the request
                    do {
                        try await connection.send(requestData)
                    } catch {
                        // If send fails, remove pending response and resume with error
                        if self.removePendingResponse(id: request.id) != nil {
                            continuation.resume(throwing: error)
                        }
                    }
                }
            }
        }

        return requestTask
    }

    /// Send a request and await its response
    private func sendAndAwait<M: Method>(_ request: Request<M>) async throws -> M.Result {
        let task = try send(request)
        return try await task.value
    }

    private func addPendingResponse<T: Sendable & Decodable>(
        id: ID,
        continuation: CheckedContinuation<T, Swift.Error>,
        type: T.Type
    ) -> MCPError? {
        if isClosing {
            return MCPError.connectionClosed
        }
        pendingRequests[id] = AnyPendingRequest(
            PendingRequest(continuation: continuation)
        )
        return nil
    }

    private func removePendingResponse(id: ID) -> AnyPendingRequest? {
        return pendingRequests.removeValue(forKey: id)
    }

    private func drainPendingResponses(throwing error: Swift.Error) {
        let pendingRequestsToDrain = pendingRequests
        pendingRequests = [:]
        for (_, request) in pendingRequestsToDrain {
            request.resume(throwing: error)
        }
    }

    private func markConnectionClosing() {
        if !isClosing {
            connectionGeneration &+= 1
        }
        isClosing = true
    }

    private func closeConnectionAfterTerminalEvent(throwing error: Swift.Error) async {
        if let connectionCloseTask {
            await connectionCloseTask.value
            return
        }
        guard !isClosing else { return }

        markConnectionClosing()
        let connectionToClose = connection
        connection = nil

        drainPendingResponses(throwing: error)
        cancelDispatchedRequests()

        let tasksToCancel = pendingRequestTasks.values
        pendingRequestTasks = [:]
        for task in tasksToCancel {
            task.cancel()
        }

        let closeTask = Task {
            if let connectionToClose {
                await connectionToClose.disconnect()
            }
        }
        connectionCloseTask = closeTask
        await closeTask.value
        connectionCloseTask = nil
    }

    // MARK: - Sampling

    /// Request sampling from the connected client
    ///
    /// Sampling allows servers to request LLM completions through the client,
    /// enabling sophisticated agentic behaviors while maintaining human-in-the-loop control.
    ///
    /// The sampling flow follows these steps:
    /// 1. Server sends a `sampling/createMessage` request to the client
    /// 2. Client reviews the request and can modify it
    /// 3. Client samples from an LLM
    /// 4. Client reviews the completion
    /// 5. Client returns the result to the server
    ///
    /// - Parameters:
    ///   - messages: The conversation history to send to the LLM
    ///   - modelPreferences: Model selection preferences
    ///   - systemPrompt: Optional system prompt
    ///   - includeContext: What MCP context to include
    ///   - temperature: Controls randomness (0.0 to 1.0)
    ///   - maxTokens: Maximum tokens to generate
    ///   - stopSequences: Array of sequences that stop generation
    ///   - _meta: Optional request metadata
    /// - Returns: The sampling result containing the model used, stop reason, role, and content
    /// - Throws: MCPError if the request fails
    /// - SeeAlso: https://modelcontextprotocol.io/docs/concepts/sampling#how-sampling-works
    public func requestSampling(
        messages: [Sampling.Message],
        modelPreferences: Sampling.ModelPreferences? = nil,
        systemPrompt: String? = nil,
        includeContext: Sampling.ContextInclusion? = nil,
        temperature: Double? = nil,
        maxTokens: Int,
        stopSequences: [String]? = nil,
        _meta: Metadata? = nil
    ) async throws -> CreateSamplingMessage.Result {
        guard connection != nil else {
            throw MCPError.internalError("Server connection not initialized")
        }

        try validateClientCapability(\.sampling, "Sampling")

        let request = CreateSamplingMessage.request(
            .init(
                messages: messages,
                modelPreferences: modelPreferences,
                systemPrompt: systemPrompt,
                includeContext: includeContext,
                temperature: temperature,
                maxTokens: maxTokens,
                stopSequences: stopSequences,
                _meta: _meta
            )
        )

        let result = try await sendAndAwait(request)
        return result
    }

    // MARK: - Elicitation

    /// Request user input from the client using form-based elicitation
    ///
    /// Elicitation allows servers to request user input during operations.
    /// This is useful for collecting user feedback, confirmations, or data
    /// that the server needs but doesn't have.
    ///
    /// The flow:
    /// 1. Server requests elicitation with a message and optional schema
    /// 2. Client displays the request to the user
    /// 3. User provides input or declines
    /// 4. Client returns the result to the server
    ///
    /// - Parameters:
    ///   - message: The message to display to the user
    ///   - mode: The elicitation mode (form or url)
    ///   - requestedSchema: Optional JSON schema describing the expected response
    ///   - _meta: Optional request metadata
    /// - Returns: The elicitation result containing the action and optional content
    /// - Throws: MCPError if the request fails
    /// - SeeAlso: https://modelcontextprotocol.io/docs/concepts/elicitation
    public func requestElicitation(
        message: String,
        requestedSchema: Elicitation.RequestSchema,
        mode: Elicitation.Mode? = nil,
        _meta: Metadata? = nil
    ) async throws -> CreateElicitation.Result {
        guard connection != nil else {
            throw MCPError.internalError("Server connection not initialized")
        }

        try validateClientCapability(\.elicitation, "Elicitation")

        let request = CreateElicitation.request(
            .form(
                .init(
                    message: message,
                    mode: mode,
                    requestedSchema: requestedSchema,
                    _meta: _meta
                )
            )
        )

        let result = try await sendAndAwait(request)
        return result
    }

    /// Request user input from the client using URL-based elicitation
    ///
    /// URL-based elicitation directs the user to an external URL for authentication
    /// or data collection. This is useful for OAuth flows or other web-based input.
    ///
    /// - Parameters:
    ///   - message: The message to display to the user
    ///   - url: The URL to direct the user to
    ///   - elicitationId: Unique identifier for this elicitation
    ///   - _meta: Optional request metadata
    /// - Returns: The elicitation result containing the action and optional content
    /// - Throws: MCPError if the request fails
    /// - SeeAlso: https://modelcontextprotocol.io/docs/concepts/elicitation
    public func requestElicitation(
        message: String,
        url: String,
        elicitationId: String,
        _meta: Metadata? = nil
    ) async throws -> CreateElicitation.Result {
        guard connection != nil else {
            throw MCPError.internalError("Server connection not initialized")
        }

        try validateClientCapability(\.elicitation, "Elicitation")

        let request = CreateElicitation.request(
            .url(
                .init(
                    message: message,
                    url: url,
                    elicitationId: elicitationId,
                    _meta: _meta
                )
            )
        )

        let result = try await sendAndAwait(request)
        return result
    }

    // MARK: - Logging

    /// Send a log message notification to connected clients.
    ///
    /// Servers that declare the `logging` capability can send structured log messages
    /// to clients. The client controls which severity levels it wants to receive via
    /// the `logging/setLevel` request.
    ///
    /// - Parameters:
    ///   - level: The severity level of the log message
    ///   - logger: Optional logger name to identify the source
    ///   - data: Arbitrary JSON-serializable data for the log message
    /// - Throws: MCPError if the server is not connected
    /// - SeeAlso: https://modelcontextprotocol.io/specification/2025-11-25/server/utilities/logging/
    public func log(
        level: LogLevel,
        logger: String? = nil,
        data: Value
    ) async throws {
        let notification = LogMessageNotification.message(
            .init(level: level, logger: logger, data: data)
        )
        try await notify(notification)
    }

    /// Send a log message notification with codable data.
    ///
    /// Convenience method that encodes data to JSON before sending.
    ///
    /// - Parameters:
    ///   - level: The severity level of the log message
    ///   - logger: Optional logger name to identify the source
    ///   - data: Any codable data for the log message
    /// - Throws: MCPError if the server is not connected or encoding fails
    /// - SeeAlso: https://modelcontextprotocol.io/specification/2025-11-25/server/utilities/logging/
    public func log<T: Codable>(
        level: LogLevel,
        logger: String? = nil,
        data: T
    ) async throws {
        let value = try Value(data)
        try await log(level: level, logger: logger, data: value)
    }

    // MARK: - Roots

    /// Request the list of roots from the connected client
    ///
    /// Roots define filesystem boundaries that servers can operate within.
    /// The client must have declared the `roots` capability and registered
    /// a roots handler for this to work.
    ///
    /// - Returns: Array of Root objects representing accessible directories/files
    /// - Throws: MCPError if the client doesn't support roots or request fails
    /// - SeeAlso: https://modelcontextprotocol.io/specification/2025-11-25/client/roots
    public func listRoots() async throws -> [Root] {
        guard connection != nil else {
            throw MCPError.internalError("Server connection not initialized")
        }

        try validateClientCapability(\.roots, "Roots")

        let request = ListRoots.request()
        let result = try await sendAndAwait(request)
        return result.roots
    }

    /// A JSON-RPC batch containing multiple requests and/or notifications
    struct Batch: Sendable {
        /// An item in a JSON-RPC batch
        enum Item: Sendable {
            case request(Request<AnyMethod>)
            case notification(Message<AnyNotification>)

        }

        var items: [Item]

        init(items: [Item]) {
            self.items = items
        }
    }

    /// Process a batch of requests and/or notifications
    private func handleBatch(_ batch: Batch, startedAt: Date = Date()) async -> Bool {
        await logger?.trace("Processing batch request", metadata: ["size": "\(batch.items.count)"])

        if batch.items.isEmpty {
            // Empty batch is invalid according to JSON-RPC spec
            let error = MCPError.invalidRequest("Batch array must not be empty")
            let response = AnyMethod.response(id: .random, error: error)
            return await sendResponse(
                response,
                context: ResponseSendContext(
                    phase: "batch_error_response",
                    requestID: nil,
                    method: nil,
                    toolName: nil,
                    responseCount: 1,
                    startedAt: startedAt
                )
            )
        }

        // Process each item in the batch and collect responses
        var responses: [Response<AnyMethod>] = []

        for item in batch.items {
            do {
                switch item {
                case .request(let request):
                    // For batched requests, collect responses instead of sending immediately
                    if let response = await handleRequest(
                        request,
                        sendResponse: false,
                        startedAt: startedAt
                    ) {
                        responses.append(response)
                    }

                case .notification(let notification):
                    // Handle notification (no response needed)
                    try await handleMessage(notification)
                }
            } catch {
                // Only add errors to response for requests (notifications don't have responses)
                if case .request(let request) = item {
                    let mcpError =
                        error as? MCPError ?? MCPError.internalError(error.localizedDescription)
                    responses.append(AnyMethod.response(id: request.id, error: mcpError))
                }
            }
        }

        // Send collected responses if any. A batch has one response-delivery attempt.
        if !responses.isEmpty {
            let responseData: Data
            do {
                let encoder = JSONEncoder()
                encoder.outputFormatting = [.sortedKeys, .withoutEscapingSlashes]
                responseData = try encoder.encode(responses)
            } catch {
                await handleResponseSendFailure(
                    error,
                    provenance: "response_send_failed",
                    context: ResponseSendContext(
                        phase: "batch_response_encode",
                        requestID: nil,
                        method: nil,
                        toolName: nil,
                        responseCount: responses.count,
                        startedAt: startedAt
                    ),
                    byteCount: nil,
                    sendStartedAt: startedAt
                )
                return false
            }

            let sent = await sendResponseData(
                responseData,
                context: ResponseSendContext(
                    phase: "batch_response",
                    requestID: nil,
                    method: nil,
                    toolName: nil,
                    responseCount: responses.count,
                    startedAt: startedAt
                )
            )
            if !sent {
                return false
            }
        }

        return true
    }

    // MARK: - Request and Message Handling

    /// Handle a request and either send the response immediately or return it.
    ///
    /// Handler execution and response delivery are intentionally separated so a
    /// completed handler result is never converted into a second error response
    /// when the transport fails while sending.
    ///
    /// - Parameters:
    ///   - request: The request to handle
    ///   - sendResponse: Whether to send the response immediately (true) or return it (false)
    /// - Returns: The response when sendResponse is false
    private func handleRequest(
        _ request: Request<AnyMethod>,
        sendResponse: Bool = true,
        startedAt: Date = Date()
    ) async -> Response<AnyMethod>? {
        let response = await makeResponse(for: request, sendResponse: sendResponse)

        guard let response else {
            return nil
        }

        if sendResponse {
            _ = await self.sendResponse(
                response,
                context: ResponseSendContext(
                    phase: "single_response",
                    requestID: request.id,
                    method: request.method,
                    toolName: toolName(from: request),
                    responseCount: 1,
                    startedAt: startedAt
                )
            )
            return nil
        }

        return response
    }

    private func makeResponse(
        for request: Request<AnyMethod>,
        sendResponse: Bool
    ) async -> Response<AnyMethod>? {
        // Check if this is a pre-processed error request (empty method)
        if request.method.isEmpty && !sendResponse {
            // This is a placeholder for an invalid request that couldn't be parsed in batch mode
            return AnyMethod.response(
                id: request.id,
                error: MCPError.invalidRequest("Invalid batch item format")
            )
        }

        await logger?.trace(
            "Processing request",
            metadata: [
                "method": "\(request.method)",
                "id": "\(request.id)",
            ])

        do {
            if configuration.strict {
                // The client SHOULD NOT send requests other than pings
                // before the server has responded to the initialize request.
                switch request.method {
                case Initialize.name, Ping.name:
                    break
                default:
                    try checkInitialized()
                }
            }

            // Find handler for method name
            guard let handler = methodHandlers[request.method] else {
                let error = MCPError.methodNotFound("Unknown method: \(request.method)")
                return AnyMethod.response(id: request.id, error: error)
            }

            // Create a task to handle the request with cancellation support.
            // Set currentRequestID as a task local so handlers can identify the active request.
            let handlerTask = Server.$currentRequestID.withValue(request.id) {
                Task<Response<AnyMethod>, Error> {
                    do {
                        // Check if task was cancelled before starting
                        try Task.checkCancellation()

                        // Handle request and get response
                        let response = try await handler(request)
                        return response
                    } catch is CancellationError {
                        // Request was cancelled, don't send a response per MCP spec
                        await logger?.debug(
                            "Request cancelled",
                            metadata: ["id": "\(request.id)", "method": "\(request.method)"]
                        )
                        throw CancellationError()
                    } catch {
                        let mcpError =
                            error as? MCPError ?? MCPError.internalError(error.localizedDescription)
                        return AnyMethod.response(id: request.id, error: mcpError)
                    }
                }
            }

            // Store the handler task for potential cancellation
            pendingRequestTasks[request.id] = handlerTask

            // Ensure cleanup happens regardless of success or failure
            defer {
                pendingRequestTasks.removeValue(forKey: request.id)
            }

            do {
                return try await handlerTask.value
            } catch is CancellationError {
                // Request was cancelled, don't send a response per MCP spec
                return nil
            } catch {
                // This should not happen as errors are caught in the task
                let mcpError = error as? MCPError ?? MCPError.internalError(error.localizedDescription)
                return AnyMethod.response(id: request.id, error: mcpError)
            }
        } catch {
            let mcpError = error as? MCPError ?? MCPError.internalError(error.localizedDescription)
            return AnyMethod.response(id: request.id, error: mcpError)
        }
    }

    private func handleMessage(_ message: Message<AnyNotification>) async throws {
        await logger?.trace(
            "Processing notification",
            metadata: ["method": "\(message.method)"])

        if configuration.strict {
            // Check initialization state unless this is an initialized notification
            if message.method != InitializedNotification.name {
                try checkInitialized()
            }
        }

        // Find notification handlers for this method
        guard let handlers = notificationHandlers[message.method] else { return }

        // Convert notification parameters to concrete type and call handlers
        for handler in handlers {
            do {
                try await handler(message)
            } catch {
                await logger?.error(
                    "Error handling notification",
                    metadata: [
                        "method": "\(message.method)",
                        "error": "\(error)",
                    ])
            }
        }
    }

    private func handleResponse(_ response: Response<AnyMethod>) async {
        if let pendingRequest = self.removePendingResponse(id: response.id) {
            switch response.result {
            case .success(let value):
                pendingRequest.resume(returning: value)
            case .failure(let error):
                pendingRequest.resume(throwing: error)
            }
        } else {
            await logger?.warning(
                "Received response for unknown request",
                metadata: ["id": "\(response.id)"]
            )
        }
    }

    private func checkInitialized() throws {
        guard isInitialized else {
            throw MCPError.invalidRequest("Server is not initialized")
        }
    }

    /// Validate the client capabilities.
    /// Throws an error if the server is configured to be strict and the capability is not supported.
    private func validateClientCapability<T>(
        _ keyPath: KeyPath<Client.Capabilities, T?>,
        _ name: String
    )
        throws
    {
        if configuration.strict {
            guard let capabilities = clientCapabilities else {
                throw MCPError.methodNotFound("Client capabilities not initialized")
            }
            guard capabilities[keyPath: keyPath] != nil else {
                throw MCPError.methodNotFound("\(name) is not supported by the client")
            }
        }
    }

    private func registerDefaultHandlers(
        initializeHook: (@Sendable (Client.Info, Client.Capabilities) async throws -> Void)?
    ) {
        // Initialize
        withMethodHandler(Initialize.self) { [weak self] params in
            guard let self = self else {
                throw MCPError.internalError("Server was deallocated")
            }

            guard await !self.isInitialized else {
                throw MCPError.invalidRequest("Server is already initialized")
            }

            // Call initialization hook if registered
            if let hook = initializeHook {
                try await hook(params.clientInfo, params.capabilities)
            }

            // Perform version negotiation
            let clientRequestedVersion = params.protocolVersion
            let negotiatedProtocolVersion = Version.negotiate(
                clientRequestedVersion: clientRequestedVersion)

            // Set initial state with the negotiated protocol version
            await self.setInitialState(
                clientInfo: params.clientInfo,
                clientCapabilities: params.capabilities,
                protocolVersion: negotiatedProtocolVersion
            )

            return Initialize.Result(
                protocolVersion: negotiatedProtocolVersion,
                capabilities: await self.capabilities,
                serverInfo: self.serverInfo,
                instructions: self.instructions
            )
        }

        // Ping
        withMethodHandler(Ping.self) { _ in return Empty() }
    }

    private func setInitialState(
        clientInfo: Client.Info,
        clientCapabilities: Client.Capabilities,
        protocolVersion: String
    ) async {
        self.clientInfo = clientInfo
        self.clientCapabilities = clientCapabilities
        self.protocolVersion = protocolVersion
        self.isInitialized = true
    }

    /// Cancel and remove a pending request task
    private func removePendingRequest(id: ID) -> Task<Response<AnyMethod>, Error>? {
        pendingRequestTasks.removeValue(forKey: id)
    }

    private func registerCancellationHandler() {
        onNotification(CancelledNotification.self) { [weak self] message in
            guard let self = self else { return }

            let requestId = message.params.requestId
            let reason = message.params.reason

            await self.logger?.debug(
                "Received cancellation notification",
                metadata: [
                    "requestId": requestId.map { "\($0)" } ?? "none",
                    "reason": reason.map { "\($0)" } ?? "none",
                ]
            )

            guard let requestId = requestId else {
                await self.logger?.warning(
                    "Received cancellation notification with no requestId (violates spec MUST)",
                    metadata: ["reason": reason.map { "\($0)" } ?? "none"]
                )
                return
            }

            // Cancel the pending request task if it exists and remove from tracking
            if let task = await self.removePendingRequest(id: requestId) {
                task.cancel()
                await self.logger?.debug(
                    "Cancelled request",
                    metadata: ["requestId": "\(requestId)"]
                )
            } else {
                // Request may have already completed or is unknown
                // Per MCP spec, we should ignore this gracefully
                await self.logger?.trace(
                    "Cancellation notification for unknown or completed request",
                    metadata: ["requestId": "\(requestId)"]
                )
            }
        }
    }

    /// Cancel a request by sending a CancelledNotification to the client.
    ///
    /// This is used when the server needs to cancel an in-progress request it made to the client
    /// (e.g., a sampling request).
    ///
    /// According to the MCP specification, cancellation is advisory:
    /// - The client SHOULD stop processing and free resources
    /// - The client MAY ignore the cancellation if the request is unknown, already completed,
    ///   or cannot be cancelled
    /// - The server SHOULD ignore any response that arrives after cancellation
    ///
    /// - Parameters:
    ///   - requestID: The ID of the request to cancel
    ///   - reason: An optional human-readable reason for the cancellation
    /// - Throws: MCPError if the notification cannot be sent
    /// - SeeAlso: https://modelcontextprotocol.io/specification/2025-11-25/basic/utilities/cancellation
    public func cancelRequest(_ requestID: ID, reason: String? = nil) async throws {
        // Send cancellation notification to client
        let notification = CancelledNotification.message(
            .init(requestId: requestID, reason: reason)
        )
        try await notify(notification)
    }
}

extension Server.Batch: Codable {
    init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()

        let encoder = JSONEncoder()
        let decoder = JSONDecoder()

        var items: [Item] = []
        for item in try container.decode([Value].self) {
            let data = try encoder.encode(item)
            try items.append(decoder.decode(Item.self, from: data))
        }

        self.items = items
    }

    func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        try container.encode(items)
    }
}

extension Server.Batch.Item: Codable {
    private enum CodingKeys: String, CodingKey {
        case id
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        // Check if it's a request (has id) or notification (no id)
        if container.contains(.id) {
            self = .request(try Request<AnyMethod>(from: decoder))
        } else {
            self = .notification(try Message<AnyNotification>(from: decoder))
        }
    }

    func encode(to encoder: Encoder) throws {
        switch self {
        case .request(let request):
            try request.encode(to: encoder)
        case .notification(let notification):
            try notification.encode(to: encoder)
        }
    }
}
