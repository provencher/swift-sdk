import Foundation
import Testing

@testable import MCP

@Suite("Client Tests", .timeLimit(.minutes(1)))
struct ClientTests {
    @Test("Client connect and disconnect")
    func testClientConnectAndDisconnect() async throws {
        let transport = MockTransport()
        let client = Client(name: "TestClient", version: "1.0")

        #expect(await transport.isConnected == false)

        // Set up a task to handle the initialize response
        let initTask = Task {
            try await Task.sleep(for: .milliseconds(10))
            if let lastMessage = await transport.sentMessages.last,
                let data = lastMessage.data(using: .utf8),
                let request = try? JSONDecoder().decode(Request<Initialize>.self, from: data)
            {
                let response = Initialize.response(
                    id: request.id,
                    result: .init(
                        protocolVersion: Version.latest,
                        capabilities: .init(),
                        serverInfo: .init(name: "TestServer", version: "1.0"),
                        instructions: nil
                    )
                )
                try await transport.queue(response: response)
            }
        }

        let result = try await client.connect(transport: transport)
        #expect(await transport.isConnected == true)
        #expect(result.protocolVersion == Version.latest)
        await client.disconnect()
        #expect(await transport.isConnected == false)
        initTask.cancel()
    }

    @Test(
        "Ping request",
        .timeLimit(.minutes(1))
    )
    func testClientPing() async throws {
        let transport = MockTransport()
        let client = Client(name: "TestClient", version: "1.0")

        // Queue a response for the initialize request
        try await Task.sleep(for: .milliseconds(10))  // Wait for request to be sent

        if let lastMessage = await transport.sentMessages.last,
            let data = lastMessage.data(using: .utf8),
            let request = try? JSONDecoder().decode(Request<Initialize>.self, from: data)
        {
            // Create a valid initialize response
            let response = Initialize.response(
                id: request.id,
                result: .init(
                    protocolVersion: Version.latest,
                    capabilities: .init(),
                    serverInfo: .init(name: "TestServer", version: "1.0"),
                    instructions: nil
                )
            )

            try await transport.queue(response: response)

            // Now complete the connect call which will automatically initialize
            let result = try await client.connect(transport: transport)
            #expect(result.protocolVersion == Version.latest)
            #expect(result.serverInfo.name == "TestServer")
            #expect(result.serverInfo.version == "1.0")

            // Small delay to ensure message loop is started
            try await Task.sleep(for: .milliseconds(10))

            // Create a task for the ping
            let pingTask = Task {
                try await client.ping()
            }

            // Give it a moment to send the request
            try await Task.sleep(for: .milliseconds(10))

            #expect(await transport.sentMessages.count == 2)  // Initialize + Ping
            #expect(await transport.sentMessages.last?.contains(Ping.name) == true)

            // Cancel the ping task
            pingTask.cancel()
        }

        // Disconnect client to clean up message loop and give time for continuation cleanup
        await client.disconnect()
        try await Task.sleep(for: .milliseconds(50))
    }

    @Test("Connection failure handling")
    func testClientConnectionFailure() async {
        let transport = MockTransport()
        await transport.setFailConnect(true)
        let client = Client(name: "TestClient", version: "1.0")

        do {
            try await client.connect(transport: transport)
            #expect(Bool(false), "Expected connection to fail")
        } catch let error as MCPError {
            if case MCPError.transportError = error {
                #expect(Bool(true))
            } else {
                #expect(Bool(false), "Expected transport error")
            }
        } catch {
            #expect(Bool(false), "Expected MCP.Error")
        }
    }

    @Test("Send failure handling")
    func testClientSendFailure() async throws {
        let transport = MockTransport()
        let client = Client(name: "TestClient", version: "1.0")

        // Set up a task to handle the initialize response
        let initTask = Task {
            try await Task.sleep(for: .milliseconds(10))
            if let lastMessage = await transport.sentMessages.last,
                let data = lastMessage.data(using: .utf8),
                let request = try? JSONDecoder().decode(Request<Initialize>.self, from: data)
            {
                let response = Initialize.response(
                    id: request.id,
                    result: .init(
                        protocolVersion: Version.latest,
                        capabilities: .init(),
                        serverInfo: .init(name: "TestServer", version: "1.0"),
                        instructions: nil
                    )
                )
                try await transport.queue(response: response)
            }
        }

        // Connect first without failure
        try await client.connect(transport: transport)
        try await Task.sleep(for: .milliseconds(10))
        initTask.cancel()

        // Now set the transport to fail sends
        await transport.setFailSend(true)

        do {
            try await client.ping()
            #expect(Bool(false), "Expected ping to fail")
        } catch let error as MCPError {
            if case MCPError.transportError = error {
                #expect(Bool(true))
            } else {
                #expect(Bool(false), "Expected transport error, got \(error)")
            }
        } catch {
            #expect(Bool(false), "Expected MCP.Error")
        }

        await client.disconnect()
    }

    @Test("Strict configuration - capabilities check")
    func testStrictConfiguration() async throws {
        let transport = MockTransport()
        let config = Client.Configuration.strict
        let client = Client(name: "TestClient", version: "1.0", configuration: config)

        // Set up a task to handle the initialize response
        let initTask = Task {
            try await Task.sleep(for: .milliseconds(10))
            if let lastMessage = await transport.sentMessages.last,
                let data = lastMessage.data(using: .utf8),
                let request = try? JSONDecoder().decode(Request<Initialize>.self, from: data)
            {
                let response = Initialize.response(
                    id: request.id,
                    result: .init(
                        protocolVersion: Version.latest,
                        capabilities: .init(),
                        serverInfo: .init(name: "TestServer", version: "1.0"),
                        instructions: nil
                    )
                )
                try await transport.queue(response: response)
            }
        }

        try await client.connect(transport: transport)

        // Create a task for listPrompts
        let promptsTask = Task<Void, Swift.Error> {
            do {
                _ = try await client.listPrompts()
                #expect(Bool(false), "Expected listPrompts to fail in strict mode")
            } catch let error as MCPError {
                if case MCPError.methodNotFound = error {
                    #expect(Bool(true))
                } else {
                    #expect(Bool(false), "Expected methodNotFound error, got \(error)")
                }
            } catch {
                #expect(Bool(false), "Expected MCP.Error")
            }
        }

        // Give it a short time to execute the task
        try await Task.sleep(for: .milliseconds(50))

        // Cancel the task if it's still running
        promptsTask.cancel()
        initTask.cancel()

        // Disconnect client
        await client.disconnect()
        try await Task.sleep(for: .milliseconds(50))
    }

    @Test("Non-strict configuration - capabilities check")
    func testNonStrictConfiguration() async throws {
        let transport = MockTransport()
        let config = Client.Configuration.default
        let client = Client(name: "TestClient", version: "1.0", configuration: config)

        // Set up a task to handle the initialize response
        let initTask = Task {
            try await Task.sleep(for: .milliseconds(10))
            if let lastMessage = await transport.sentMessages.last,
                let data = lastMessage.data(using: .utf8),
                let request = try? JSONDecoder().decode(Request<Initialize>.self, from: data)
            {
                let response = Initialize.response(
                    id: request.id,
                    result: .init(
                        protocolVersion: Version.latest,
                        capabilities: .init(),
                        serverInfo: .init(name: "TestServer", version: "1.0"),
                        instructions: nil
                    )
                )
                try await transport.queue(response: response)
            }
        }

        try await client.connect(transport: transport)

        // Make sure init task is complete
        initTask.cancel()

        // Wait a bit for any setup to complete
        try await Task.sleep(for: .milliseconds(10))

        // Send the listPrompts request and immediately provide an error response
        let promptsTask = Task {
            do {
                // Start the request
                try await Task.sleep(for: .seconds(1))

                // Get the last sent message and extract the request ID
                if let lastMessage = await transport.sentMessages.last,
                    let data = lastMessage.data(using: .utf8),
                    let decodedRequest = try? JSONDecoder().decode(
                        Request<ListPrompts>.self, from: data)
                {

                    // Create an error response with the same ID
                    let errorResponse = Response<ListPrompts>(
                        id: decodedRequest.id,
                        error: MCPError.methodNotFound("Test: Prompts capability not available")
                    )
                    try await transport.queue(response: errorResponse)

                    // Try the request now that we have a response queued
                    do {
                        _ = try await client.listPrompts()
                        #expect(Bool(false), "Expected listPrompts to fail in non-strict mode")
                    } catch let error as MCPError {
                        if case MCPError.methodNotFound = error {
                            #expect(Bool(true))
                        } else {
                            #expect(Bool(false), "Expected methodNotFound error, got \(error)")
                        }
                    } catch {
                        #expect(Bool(false), "Expected MCP.Error")
                    }
                }
            } catch {
                // Ignore task cancellation
                if !(error is CancellationError) {
                    throw error
                }
            }
        }

        // Wait for the task to complete or timeout
        let timeoutTask = Task {
            try await Task.sleep(for: .milliseconds(500))
            promptsTask.cancel()
        }

        // Wait for the task to complete
        _ = await promptsTask.result

        // Cancel the timeout task
        timeoutTask.cancel()

        // Disconnect client
        await client.disconnect()
    }

    @Test("Batch request - success")
    func testBatchRequestSuccess() async throws {
        let transport = MockTransport()
        let client = Client(name: "TestClient", version: "1.0")

        // Set up a task to handle the initialize response
        let initTask = Task {
            try await Task.sleep(for: .milliseconds(10))
            if let lastMessage = await transport.sentMessages.last,
                let data = lastMessage.data(using: .utf8),
                let request = try? JSONDecoder().decode(Request<Initialize>.self, from: data)
            {
                let response = Initialize.response(
                    id: request.id,
                    result: .init(
                        protocolVersion: Version.latest,
                        capabilities: .init(),
                        serverInfo: .init(name: "TestServer", version: "1.0"),
                        instructions: nil
                    )
                )
                try await transport.queue(response: response)
            }
        }

        try await client.connect(transport: transport)
        try await Task.sleep(for: .milliseconds(10))  // Allow connection tasks
        initTask.cancel()

        let request1 = Ping.request()
        let request2 = Ping.request()
        nonisolated(unsafe) var resultTask1: Task<Ping.Result, Swift.Error>?
        nonisolated(unsafe) var resultTask2: Task<Ping.Result, Swift.Error>?

        try await client.withBatch { batch in
            resultTask1 = try await batch.addRequest(request1)
            resultTask2 = try await batch.addRequest(request2)
        }

        // Check if batch message was sent (after initialize and initialized notification)
        let sentMessages = await transport.sentMessages
        #expect(sentMessages.count == 3)  // Initialize request + Initialized notification + Batch

        guard let batchData = sentMessages.last?.data(using: .utf8) else {
            #expect(Bool(false), "Failed to get batch data")
            return
        }

        // Verify the sent batch contains the two requests
        let decoder = JSONDecoder()
        let sentRequests = try decoder.decode([AnyRequest].self, from: batchData)
        #expect(sentRequests.count == 2)
        #expect(sentRequests.first?.id == request1.id)
        #expect(sentRequests.first?.method == Ping.name)
        #expect(sentRequests.last?.id == request2.id)
        #expect(sentRequests.last?.method == Ping.name)

        // Prepare batch response
        let response1 = Response<Ping>(id: request1.id, result: .init())
        let response2 = Response<Ping>(id: request2.id, result: .init())
        let anyResponse1 = try AnyResponse(response1)
        let anyResponse2 = try AnyResponse(response2)

        // Queue the batch response
        try await transport.queue(batch: [anyResponse1, anyResponse2])

        // Wait for results and verify
        guard let task1 = resultTask1, let task2 = resultTask2 else {
            #expect(Bool(false), "Result tasks not created")
            return
        }

        _ = try await task1.value  // Should succeed
        _ = try await task2.value  // Should succeed

        #expect(Bool(true))  // Reaching here means success

        await client.disconnect()
    }

    @Test("Batch request - mixed success/error")
    func testBatchRequestMixed() async throws {
        let transport = MockTransport()
        let client = Client(name: "TestClient", version: "1.0")

        // Set up a task to handle the initialize response
        let initTask = Task {
            try await Task.sleep(for: .milliseconds(10))
            if let lastMessage = await transport.sentMessages.last,
                let data = lastMessage.data(using: .utf8),
                let request = try? JSONDecoder().decode(Request<Initialize>.self, from: data)
            {
                let response = Initialize.response(
                    id: request.id,
                    result: .init(
                        protocolVersion: Version.latest,
                        capabilities: .init(),
                        serverInfo: .init(name: "TestServer", version: "1.0"),
                        instructions: nil
                    )
                )
                try await transport.queue(response: response)
            }
        }

        try await client.connect(transport: transport)
        try await Task.sleep(for: .milliseconds(10))
        initTask.cancel()

        let request1 = Ping.request()  // Success
        let request2 = Ping.request()  // Error

        nonisolated(unsafe) var resultTasks: [Task<Ping.Result, Swift.Error>] = []

        try await client.withBatch { batch in
            resultTasks.append(try await batch.addRequest(request1))
            resultTasks.append(try await batch.addRequest(request2))
        }

        // Check if batch message was sent (after initialize and initialized notification)
        #expect(await transport.sentMessages.count == 3)  // Initialize request + Initialized notification + Batch

        // Prepare batch response (success for 1, error for 2)
        let response1 = Response<Ping>(id: request1.id, result: .init())
        let error = MCPError.internalError("Simulated batch error")
        let response2 = Response<Ping>(id: request2.id, error: error)
        let anyResponse1 = try AnyResponse(response1)
        let anyResponse2 = try AnyResponse(response2)

        // Queue the batch response
        try await transport.queue(batch: [anyResponse1, anyResponse2])

        // Wait for results and verify
        #expect(resultTasks.count == 2)
        guard resultTasks.count == 2 else {
            #expect(Bool(false), "Expected 2 result tasks")
            return
        }

        let task1 = resultTasks[0]
        let task2 = resultTasks[1]

        _ = try await task1.value  // Task 1 should succeed

        do {
            _ = try await task2.value  // Task 2 should fail
            #expect(Bool(false), "Task 2 should have thrown an error")
        } catch let mcpError as MCPError {
            if case .internalError(let message) = mcpError {
                #expect(message == "Simulated batch error")
            } else {
                #expect(Bool(false), "Expected internalError, got \(mcpError)")
            }
        } catch {
            #expect(Bool(false), "Expected MCPError, got \(error)")
        }

        await client.disconnect()
    }

    @Test("Batch request - empty")
    func testBatchRequestEmpty() async throws {
        let transport = MockTransport()
        let client = Client(name: "TestClient", version: "1.0")

        // Set up a task to handle the initialize response
        let initTask = Task {
            try await Task.sleep(for: .milliseconds(10))
            if let lastMessage = await transport.sentMessages.last,
                let data = lastMessage.data(using: .utf8),
                let request = try? JSONDecoder().decode(Request<Initialize>.self, from: data)
            {
                let response = Initialize.response(
                    id: request.id,
                    result: .init(
                        protocolVersion: Version.latest,
                        capabilities: .init(),
                        serverInfo: .init(name: "TestServer", version: "1.0"),
                        instructions: nil
                    )
                )
                try await transport.queue(response: response)
            }
        }

        try await client.connect(transport: transport)
        try await Task.sleep(for: .milliseconds(10))
        initTask.cancel()

        // Call withBatch but don't add any requests
        try await client.withBatch { _ in
            // No requests added
        }

        // Check that only initialize message and initialized notification were sent
        #expect(await transport.sentMessages.count == 2)  // Initialize request + Initialized notification

        await client.disconnect()
    }

    @Test("Batch send failure resumes registered pending requests")
    func testBatchSendFailureResumesRegisteredPendingRequests() async throws {
        let transport = MockTransport()
        let client = Client(name: "TestClient", version: "1.0")
        try await connectInitialized(client, transport: transport)

        let request1 = Ping.request(id: .number(2001))
        let request2 = Ping.request(id: .number(2002))
        nonisolated(unsafe) var resultTasks: [Task<Ping.Result, Swift.Error>] = []

        await transport.setFailSend(true)

        do {
            try await client.withBatch { batch in
                resultTasks.append(try await batch.addRequest(request1))
                resultTasks.append(try await batch.addRequest(request2))
            }
            #expect(Bool(false), "Expected batch send to fail")
        } catch let error as MCPError {
            if case .transportError = error {
                #expect(Bool(true))
            } else {
                #expect(Bool(false), "Expected transportError, got \(error)")
            }
        }

        #expect(resultTasks.count == 2)
        for task in resultTasks {
            let result = await awaitResult {
                try await task.value
            }
            switch result {
            case .success:
                #expect(Bool(false), "Expected registered batch task to fail after send failure")
            case .failure(let error as MCPError):
                if case .transportError = error {
                    #expect(Bool(true))
                } else {
                    #expect(Bool(false), "Expected transportError, got \(error)")
                }
            case .failure(let error):
                #expect(Bool(false), "Expected MCPError.transportError, got \(error)")
            }
        }

        await client.disconnect()
    }

    @Test("Batch body failure resumes registered pending requests")
    func testBatchBodyFailureResumesRegisteredPendingRequests() async throws {
        struct BodyFailure: Error {}

        let transport = MockTransport()
        let client = Client(name: "TestClient", version: "1.0")
        try await connectInitialized(client, transport: transport)

        let request = Ping.request(id: .number(2003))
        nonisolated(unsafe) var resultTask: Task<Ping.Result, Swift.Error>?

        do {
            try await client.withBatch { batch in
                resultTask = try await batch.addRequest(request)
                throw BodyFailure()
            }
            #expect(Bool(false), "Expected batch body to throw")
        } catch is BodyFailure {
            #expect(Bool(true))
        }

        guard let resultTask else {
            #expect(Bool(false), "Expected batch task to be registered before body failure")
            await client.disconnect()
            return
        }

        let result = await awaitResult {
            try await resultTask.value
        }
        switch result {
        case .success:
            #expect(Bool(false), "Expected registered batch task to fail after body failure")
        case .failure(let error):
            #expect(error is BodyFailure)
        }

        await client.disconnect()
    }

    @Test("Notify method sends notifications")
    func testClientNotify() async throws {
        let transport = MockTransport()
        let client = Client(name: "TestClient", version: "1.0")

        // Set up a task to handle the initialize response
        let initTask = Task {
            try await Task.sleep(for: .milliseconds(10))
            if let lastMessage = await transport.sentMessages.last,
                let data = lastMessage.data(using: .utf8),
                let request = try? JSONDecoder().decode(Request<Initialize>.self, from: data)
            {
                let response = Initialize.response(
                    id: request.id,
                    result: .init(
                        protocolVersion: Version.latest,
                        capabilities: .init(),
                        serverInfo: .init(name: "TestServer", version: "1.0"),
                        instructions: nil
                    )
                )
                try await transport.queue(response: response)
            }
        }

        try await client.connect(transport: transport)
        try await Task.sleep(for: .milliseconds(10))
        initTask.cancel()

        // Create a test notification
        let notification = InitializedNotification.message()
        try await client.notify(notification)

        // Verify notification was sent (in addition to initialize and initialized notification)
        #expect(await transport.sentMessages.count == 3)  // Initialize request + Initialized notification + Custom notification

        if let sentMessage = await transport.sentMessages.last,
            let data = sentMessage.data(using: .utf8)
        {

            // Decode as Message<InitializedNotification>
            let decoder = JSONDecoder()
            do {
                let decodedNotification = try decoder.decode(
                    Message<InitializedNotification>.self, from: data)
                #expect(decodedNotification.method == InitializedNotification.name)
            } catch {
                #expect(Bool(false), "Failed to decode notification: \(error)")
            }
        } else {
            #expect(Bool(false), "No message was sent")
        }

        await client.disconnect()
    }

    @Test("Initialize sends initialized notification")
    func testClientInitializeNotification() async throws {
        let transport = MockTransport()
        let client = Client(name: "TestClient", version: "1.0")

        // Create a task for initialize
        let initTask = Task {
            // Queue a response for the initialize request
            try await Task.sleep(for: .milliseconds(10))  // Wait for request to be sent

            if let lastMessage = await transport.sentMessages.last,
                let data = lastMessage.data(using: .utf8),
                let request = try? JSONDecoder().decode(Request<Initialize>.self, from: data)
            {

                // Create a valid initialize response
                let response = Initialize.response(
                    id: request.id,
                    result: .init(
                        protocolVersion: Version.latest,
                        capabilities: .init(),
                        serverInfo: .init(name: "TestServer", version: "1.0"),
                        instructions: nil
                    )
                )

                try await transport.queue(response: response)

                // Now complete the initialize call
                try await client.connect(transport: transport)
                try await Task.sleep(for: .milliseconds(10))

                // Verify that two messages were sent: initialize request and initialized notification
                #expect(await transport.sentMessages.count == 2)

                // Check that the second message is the initialized notification
                let notifications = await transport.sentMessages
                if notifications.count >= 2 {
                    let notificationJson = notifications[1]
                    if let notificationData = notificationJson.data(using: .utf8) {
                        do {
                            let decoder = JSONDecoder()
                            let decodedNotification = try decoder.decode(
                                Message<InitializedNotification>.self, from: notificationData)
                            #expect(decodedNotification.method == InitializedNotification.name)
                        } catch {
                            #expect(Bool(false), "Failed to decode notification: \(error)")
                        }
                    } else {
                        #expect(Bool(false), "Could not convert notification to data")
                    }
                } else {
                    #expect(
                        Bool(false), "Expected both initialize request and initialized notification"
                    )
                }
            }
        }

        // Wait with timeout
        let timeoutTask = Task {
            try await Task.sleep(for: .seconds(1))
            initTask.cancel()
        }

        // Wait for the task to complete
        do {
            _ = try await initTask.value
        } catch is CancellationError {
            #expect(Bool(false), "Test timed out")
        } catch {
            #expect(Bool(false), "Unexpected error: \(error)")
        }

        timeoutTask.cancel()

        await client.disconnect()
    }

    @Test("Race condition between send error and response")
    func testSendErrorResponseRace() async throws {
        let transport = MockTransport()
        let client = Client(name: "TestClient", version: "1.0")

        // Set up a task to handle the initialize response
        let initTask = Task {
            try await Task.sleep(for: .milliseconds(10))
            if let lastMessage = await transport.sentMessages.last,
                let data = lastMessage.data(using: .utf8),
                let request = try? JSONDecoder().decode(Request<Initialize>.self, from: data)
            {
                let response = Initialize.response(
                    id: request.id,
                    result: .init(
                        protocolVersion: Version.latest,
                        capabilities: .init(),
                        serverInfo: .init(name: "TestServer", version: "1.0"),
                        instructions: nil
                    )
                )
                try await transport.queue(response: response)
            }
        }

        try await client.connect(transport: transport)
        try await Task.sleep(for: .milliseconds(10))
        initTask.cancel()

        // Set up the transport to fail sends from the start
        await transport.setFailSend(true)

        // Create a ping request to get the ID
        let request = Ping.request()

        // Create a response for the request and queue it immediately
        let response = Response<Ping>(id: request.id, result: .init())
        let anyResponse = try AnyResponse(response)
        try await transport.queue(response: anyResponse)

        // Now attempt to send the request - this should fail due to send error
        // but the response handler might also try to process the queued response
        do {
            _ = try await client.ping()
            #expect(Bool(false), "Expected send to fail")
        } catch let error as MCPError {
            if case .transportError = error {
                #expect(Bool(true))
            } else {
                #expect(Bool(false), "Expected transport error, got \(error)")
            }
        } catch {
            #expect(Bool(false), "Expected MCPError, got \(error)")
        }

        // Verify no continuation misuse occurred
        // (If it did, the test would have crashed)

        await client.disconnect()
    }

    @Test("Race condition between response and send error")
    func testResponseSendErrorRace() async throws {
        let transport = MockTransport()
        let client = Client(name: "TestClient", version: "1.0")

        // Set up a task to handle the initialize response
        let initTask = Task {
            try await Task.sleep(for: .milliseconds(10))
            if let lastMessage = await transport.sentMessages.last,
                let data = lastMessage.data(using: .utf8),
                let request = try? JSONDecoder().decode(Request<Initialize>.self, from: data)
            {
                let response = Initialize.response(
                    id: request.id,
                    result: .init(
                        protocolVersion: Version.latest,
                        capabilities: .init(),
                        serverInfo: .init(name: "TestServer", version: "1.0"),
                        instructions: nil
                    )
                )
                try await transport.queue(response: response)
            }
        }

        try await client.connect(transport: transport)
        try await Task.sleep(for: .milliseconds(10))
        initTask.cancel()

        // Create a ping request to get the ID
        let request = Ping.request()

        // Create a response for the request and queue it immediately
        let response = Response<Ping>(id: request.id, result: .init())
        let anyResponse = try AnyResponse(response)
        try await transport.queue(response: anyResponse)

        // Set up the transport to fail sends
        await transport.setFailSend(true)

        // Now attempt to send the request
        // The response might be processed before the send error occurs
        do {
            _ = try await client.ping()
            // In this case, the response handler won the race and the request succeeded
            #expect(Bool(true), "Response handler won the race - request succeeded")
        } catch let error as MCPError {
            if case .transportError = error {
                // In this case, the send error handler won the race
                #expect(Bool(true), "Send error handler won the race - request failed")
            } else {
                #expect(Bool(false), "Expected transport error, got \(error)")
            }
        } catch {
            #expect(Bool(false), "Expected MCPError, got \(error)")
        }

        // Verify no continuation misuse occurred
        // (If it did, the test would have crashed)

        await client.disconnect()
    }

    @Test("Clean receive EOF drains pending requests")
    func testCleanReceiveEOFDrainsPendingRequests() async throws {
        let transport = MockTransport()
        let client = Client(name: "TestClient", version: "1.0")
        try await connectInitialized(client, transport: transport)

        let context = try await client.send(Ping.request(id: .number(1001)))
        #expect(await waitForSentMessages(atLeast: 3, transport: transport))

        await transport.finishReceiving()

        let result = await awaitResult {
            try await context.value
        }

        switch result {
        case .success:
            #expect(Bool(false), "Expected pending request to fail after clean EOF")
        case .failure(let error as MCPError):
            #expect(error == .connectionClosed)
        case .failure(let error):
            #expect(Bool(false), "Expected MCPError.connectionClosed, got \(error)")
        }

        do {
            _ = try await client.send(Ping.request(id: .number(1004)))
            #expect(Bool(false), "Expected send after clean EOF to fail immediately")
        } catch let error as MCPError {
            #expect(error == .connectionClosed)
        }

        await client.disconnect()
    }

    @Test("Thrown receive error drains pending requests")
    func testReceiveErrorDrainsPendingRequests() async throws {
        let transport = MockTransport()
        let client = Client(name: "TestClient", version: "1.0")
        try await connectInitialized(client, transport: transport)

        let context = try await client.send(Ping.request(id: .number(1002)))
        #expect(await waitForSentMessages(atLeast: 3, transport: transport))

        await transport.failReceiving(with: MCPError.transportError(POSIXError(.ECONNRESET)))

        let result = await awaitResult {
            try await context.value
        }

        switch result {
        case .success:
            #expect(Bool(false), "Expected pending request to fail after receive error")
        case .failure(let error as MCPError):
            if case .transportError = error {
                #expect(Bool(true))
            } else {
                #expect(Bool(false), "Expected transportError, got \(error)")
            }
        case .failure(let error):
            #expect(Bool(false), "Expected MCPError.transportError, got \(error)")
        }

        do {
            _ = try await client.send(Ping.request(id: .number(1005)))
            #expect(Bool(false), "Expected send after receive error to fail immediately")
        } catch let error as MCPError {
            if case .transportError = error {
                #expect(Bool(true))
            } else {
                #expect(Bool(false), "Expected transportError, got \(error)")
            }
        }

        await client.disconnect()
    }

    @Test("Explicit disconnect drains pending requests")
    func testExplicitDisconnectDrainsPendingRequests() async throws {
        let transport = MockTransport()
        let client = Client(name: "TestClient", version: "1.0")
        try await connectInitialized(client, transport: transport)

        let context = try await client.send(Ping.request(id: .number(1003)))
        #expect(await waitForSentMessages(atLeast: 3, transport: transport))

        await client.disconnect()

        let result = await awaitResult {
            try await context.value
        }

        switch result {
        case .success:
            #expect(Bool(false), "Expected pending request to fail after disconnect")
        case .failure(let error as MCPError):
            #expect(error == .connectionClosed)
        case .failure(let error):
            #expect(Bool(false), "Expected MCPError.connectionClosed, got \(error)")
        }
    }

    private func connectInitialized(
        _ client: Client,
        transport: MockTransport,
        capabilities: Server.Capabilities = .init()
    ) async throws {
        let initTask = Task {
            for _ in 0..<200 {
                if let lastMessage = await transport.sentMessages.last,
                    let data = lastMessage.data(using: .utf8),
                    let request = try? JSONDecoder().decode(Request<Initialize>.self, from: data)
                {
                    let response = Initialize.response(
                        id: request.id,
                        result: .init(
                            protocolVersion: Version.latest,
                            capabilities: capabilities,
                            serverInfo: .init(name: "TestServer", version: "1.0"),
                            instructions: nil
                        )
                    )
                    try? await transport.queue(response: response)
                    return
                }
                try? await Task.sleep(for: .milliseconds(5))
            }
        }

        _ = try await client.connect(transport: transport)
        initTask.cancel()
    }

    private func waitForSentMessages(
        atLeast count: Int,
        transport: MockTransport,
        attempts: Int = 100
    ) async -> Bool {
        for _ in 0..<attempts {
            if await transport.sentMessages.count >= count {
                return true
            }
            try? await Task.sleep(for: .milliseconds(5))
        }
        return await transport.sentMessages.count >= count
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
                return .failure(MCPError.internalError("Timed out waiting for pending request drain"))
            }

            let result = await group.next()!
            group.cancelAll()
            return result
        }
    }
}
