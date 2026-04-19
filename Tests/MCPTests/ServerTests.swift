import Foundation
import Testing

@testable import MCP

private struct SlowServerTestMethod: MCP.Method {
    static let name = "test/slow"
    typealias Parameters = Empty
    typealias Result = Empty
}

@Suite("Server Tests")
struct ServerTests {
    @Test("Start and stop server")
    func testServerStartAndStop() async throws {
        let transport = MockTransport()
        let server = Server(name: "TestServer", version: "1.0")

        #expect(await transport.isConnected == false)
        try await server.start(transport: transport)
        #expect(await transport.isConnected == true)
        await server.stop()
        #expect(await transport.isConnected == false)
    }

    @Test("Initialize request handling")
    func testServerHandleInitialize() async throws {
        let transport = MockTransport()

        // Queue an initialize request
        try await transport.queue(
            request: Initialize.request(
                .init(
                    protocolVersion: Version.latest,
                    capabilities: .init(),
                    clientInfo: .init(name: "TestClient", version: "1.0")
                )
            ))

        // Start the server
        let server: Server = Server(
            name: "TestServer",
            version: "1.0"
        )
        try await server.start(transport: transport)

        // Wait for message processing and response
        try await Task.sleep(for: .milliseconds(200))

        #expect(await transport.sentMessages.count == 1)

        let messages = await transport.sentMessages
        if let response = messages.first {
            #expect(response.contains("serverInfo"))
        }

        // Clean up
        await server.stop()
        await transport.disconnect()
    }

    @Test("Initialize hook - successful")
    func testInitializeHookSuccess() async throws {
        let transport = MockTransport()

        actor TestState {
            var hookCalled = false
            func setHookCalled() { hookCalled = true }
            func wasHookCalled() -> Bool { hookCalled }
        }

        let state = TestState()
        let server = Server(name: "TestServer", version: "1.0")

        // Start with the hook directly
        try await server.start(transport: transport) { clientInfo, capabilities in
            #expect(clientInfo.name == "TestClient")
            #expect(clientInfo.version == "1.0")
            await state.setHookCalled()
        }

        // Wait for server to initialize
        try await Task.sleep(for: .milliseconds(10))

        // Queue an initialize request
        try await transport.queue(
            request: Initialize.request(
                .init(
                    protocolVersion: Version.latest,
                    capabilities: .init(),
                    clientInfo: .init(name: "TestClient", version: "1.0")
                )
            ))

        // Wait for message processing and hook execution
        try await Task.sleep(for: .milliseconds(500))

        #expect(await state.wasHookCalled() == true)
        #expect(await transport.sentMessages.count >= 1)

        let messages = await transport.sentMessages
        if let response = messages.first {
            #expect(response.contains("serverInfo"))
        }

        await server.stop()
        await transport.disconnect()
    }

    @Test("Initialize hook - rejection")
    func testInitializeHookRejection() async throws {
        let transport = MockTransport()

        let server = Server(name: "TestServer", version: "1.0")

        try await server.start(transport: transport) { clientInfo, _ in
            if clientInfo.name == "BlockedClient" {
                throw MCPError.invalidRequest("Client not allowed")
            }
        }

        // Wait for server to initialize
        try await Task.sleep(for: .milliseconds(10))

        // Queue an initialize request from blocked client
        try await transport.queue(
            request: Initialize.request(
                .init(
                    protocolVersion: Version.latest,
                    capabilities: .init(),
                    clientInfo: .init(name: "BlockedClient", version: "1.0")
                )
            ))

        // Wait for message processing
        try await Task.sleep(for: .milliseconds(200))

        #expect(await transport.sentMessages.count >= 1)

        let messages = await transport.sentMessages
        if let response = messages.first {
            #expect(response.contains("error"))
            #expect(response.contains("Client not allowed"))
        }

        await server.stop()
        await transport.disconnect()
    }

    @Test("JSON-RPC batch processing")
    func testJSONRPCBatchProcessing() async throws {
        let (clientTransport, serverTransport) = await InMemoryTransport.createConnectedPair()
        let server = Server(name: "TestServer", version: "1.0")

        // Connect transports
        try await clientTransport.connect()
        try await serverTransport.connect()

        // Start receiving messages on client side
        let receiveTask = Task {
            var responses: [String] = []
            for try await data in await clientTransport.receive() {
                if let response = String(data: data, encoding: .utf8) {
                    responses.append(response)
                }
                // Stop after receiving 2 responses (initialize + batch)
                if responses.count == 2 {
                    break
                }
            }
            return responses
        }

        // Start the server
        try await server.start(transport: serverTransport)

        // Initialize the server first
        let initRequest = Initialize.request(
            .init(
                protocolVersion: Version.latest,
                capabilities: .init(),
                clientInfo: .init(name: "TestClient", version: "1.0")
            )
        )
        let initData = try JSONEncoder().encode(AnyRequest(initRequest))
        try await clientTransport.send(initData)

        // Wait for initialization
        try await Task.sleep(for: .milliseconds(100))

        // Create a batch with multiple requests
        let batchJSON = """
            [
                {"jsonrpc":"2.0","id":1,"method":"ping","params":{}},
                {"jsonrpc":"2.0","id":2,"method":"ping","params":{}}
            ]
            """
        let batchData = batchJSON.data(using: .utf8)!
        try await clientTransport.send(batchData)

        // Wait for batch processing
        try await Task.sleep(for: .milliseconds(200))

        // Get responses
        let responses = try await receiveTask.value
        #expect(responses.count == 2)

        // Verify the batch response (second response)
        if responses.count >= 2 {
            let batchResponse = responses[1]

            // Should be an array
            #expect(batchResponse.hasPrefix("["))
            #expect(batchResponse.hasSuffix("]"))

            // Should contain both request IDs
            #expect(batchResponse.contains("\"id\":1"))
            #expect(batchResponse.contains("\"id\":2"))
        }

        await server.stop()
        await clientTransport.disconnect()
        await serverTransport.disconnect()
    }

    @Test("Response send failure after handler success is single-attempt and terminal")
    func testResponseSendFailureAfterHandlerSuccessDoesNotSendSecondError() async throws {
        let transport = MockTransport()
        await transport.setFailOnSendAttempt(1)

        let server = Server(name: "TestServer", version: "1.0")
        try await server.start(transport: transport)

        try await transport.queue(request: Ping.request(id: .number(1)))
        try await Task.sleep(for: .milliseconds(200))

        #expect(await transport.sendAttempts == 1)
        #expect(await transport.sentMessages.isEmpty)
        #expect(await transport.isConnected == false)

        await server.stop()
    }

    @Test("Batch response send failure is single-attempt and terminal")
    func testBatchResponseSendFailureIsSingleAttemptAndTerminal() async throws {
        let transport = MockTransport()
        await transport.setFailOnSendAttempt(1)

        let server = Server(name: "TestServer", version: "1.0")
        try await server.start(transport: transport)

        let batchJSON = """
            [
                {"jsonrpc":"2.0","id":1,"method":"ping","params":{}},
                {"jsonrpc":"2.0","id":2,"method":"ping","params":{}}
            ]
            """
        await transport.queue(data: batchJSON.data(using: .utf8)!)
        try await Task.sleep(for: .milliseconds(200))

        #expect(await transport.sendAttempts == 1)
        #expect(await transport.sentMessages.isEmpty)
        #expect(await transport.isConnected == false)

        await server.stop()
    }

    @Test("Parse error response send failure closes transport")
    func testParseErrorResponseSendFailureClosesTransport() async throws {
        let transport = MockTransport()
        await transport.setFailOnSendAttempt(1)

        let server = Server(name: "TestServer", version: "1.0")
        try await server.start(transport: transport)

        await transport.queue(data: Data("not-json".utf8))
        try await Task.sleep(for: .milliseconds(200))

        #expect(await transport.sendAttempts == 1)
        #expect(await transport.sentMessages.isEmpty)
        #expect(await transport.isConnected == false)

        await server.stop()
    }

    @Test("Response send failure prevents later request handling")
    func testResponseSendFailurePreventsLaterRequestHandling() async throws {
        actor Counter {
            private(set) var count = 0
            func increment() { count += 1 }
        }

        let counter = Counter()
        let transport = MockTransport()
        await transport.setFailOnSendAttempt(1)

        let server = Server(name: "TestServer", version: "1.0")
        await server.withMethodHandler(ListTools.self) { _ in
            await counter.increment()
            return ListTools.Result(tools: [])
        }
        try await transport.queue(request: ListTools.request(id: .number(1), .init()))
        try await transport.queue(request: ListTools.request(id: .number(2), .init()))

        try await server.start(transport: transport)
        try await Task.sleep(for: .milliseconds(300))

        #expect(await counter.count == 1)
        #expect(await transport.sendAttempts == 1)
        #expect(await transport.isConnected == false)

        await server.stop()
    }

    @Test("Long-running request does not block later request")
    func testLongRunningRequestDoesNotBlockLaterRequest() async throws {
        actor Counter {
            private(set) var count = 0
            func increment() { count += 1 }
        }

        let counter = Counter()
        let transport = MockTransport()
        let server = Server(name: "TestServer", version: "1.0")

        await server.withMethodHandler(SlowServerTestMethod.self) { _ in
            try await Task.sleep(for: .milliseconds(300))
            return Empty()
        }
        await server.withMethodHandler(ListTools.self) { _ in
            await counter.increment()
            return ListTools.Result(tools: [])
        }

        try await transport.queue(request: SlowServerTestMethod.request(id: .number(1)))
        try await transport.queue(request: ListTools.request(id: .number(2), .init()))

        try await server.start(transport: transport)
        try await Task.sleep(for: .milliseconds(100))

        #expect(await counter.count == 1)

        await server.stop()
    }

    @Test("Clean EOF drains server-initiated pending responses")
    func testCleanEOFDrainsServerInitiatedPendingResponses() async throws {
        let transport = MockTransport()
        let server = Server(name: "TestServer", version: "1.0")
        try await server.start(transport: transport)

        let rootsTask = Task {
            try await server.listRoots()
        }

        #expect(await waitForSentMessage(containing: ListRoots.name, transport: transport))
        await transport.finishReceiving()

        let result = await awaitResult {
            try await rootsTask.value
        }

        switch result {
        case .success:
            #expect(Bool(false), "Expected pending server request to fail after clean EOF")
        case .failure(let error as MCPError):
            #expect(error == .connectionClosed)
        case .failure(let error):
            #expect(Bool(false), "Expected MCPError.connectionClosed, got \(error)")
        }

        #expect(await transport.isConnected == false)
        await server.stop()
    }

    private func waitForSentMessage(
        containing needle: String,
        transport: MockTransport,
        attempts: Int = 100
    ) async -> Bool {
        for _ in 0..<attempts {
            if await transport.sentMessages.contains(where: { $0.contains(needle) }) {
                return true
            }
            try? await Task.sleep(for: .milliseconds(5))
        }
        return await transport.sentMessages.contains(where: { $0.contains(needle) })
    }

    private func awaitResult<T: Sendable>(
        timeout: Duration = .milliseconds(500),
        _ operation: @escaping @Sendable () async throws -> T
    ) async -> Result<T, Swift.Error> {
        await withTaskGroup(of: Result<T, Swift.Error>.self) { group in
            group.addTask {
                do {
                    return .success(try await operation())
                } catch {
                    return .failure(error)
                }
            }
            group.addTask {
                try? await Task.sleep(for: timeout)
                return .failure(MCPError.internalError("Timed out waiting for result"))
            }

            let result = await group.next()!
            group.cancelAll()
            return result
        }
    }
}
