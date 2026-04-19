import Logging

import struct Foundation.Data
import class Foundation.JSONDecoder
import class Foundation.JSONEncoder
import struct Foundation.POSIXError

@testable import MCP

/// Mock transport for testing
actor MockTransport: Transport {
    var logger: Logger

    let encoder = JSONEncoder()
    let decoder = JSONDecoder()

    var isConnected = false

    private(set) var sentData: [Data] = []
    private(set) var sendAttempts = 0
    var sentMessages: [String] {
        return sentData.compactMap { data in
            guard let string = String(data: data, encoding: .utf8) else {
                logger.error("Failed to decode sent data as UTF-8")
                return nil
            }
            return string
        }
    }

    private var dataToReceive: [Data] = []
    private(set) var receivedMessages: [String] = []

    private var dataStreamContinuation: AsyncThrowingStream<Data, Swift.Error>.Continuation?
    private var shouldFinishOnReceive = false
    private var receiveErrorOnReceive: MCPError?

    var shouldFailConnect = false
    var shouldFailSend = false
    var failOnSendAttempt: Int?

    init(logger: Logger = Logger(label: "mcp.test.transport")) {
        self.logger = logger
    }

    public func connect() async throws {
        if shouldFailConnect {
            throw MCPError.transportError(POSIXError(.ECONNREFUSED))
        }
        isConnected = true
    }

    public func disconnect() async {
        isConnected = false
        dataStreamContinuation?.finish()
        dataStreamContinuation = nil
    }

    public func send(_ message: Data) async throws {
        sendAttempts += 1
        if shouldFailSend || failOnSendAttempt == sendAttempts {
            throw MCPError.transportError(POSIXError(.EIO))
        }
        sentData.append(message)
    }

    public func receive() -> AsyncThrowingStream<Data, Swift.Error> {
        return AsyncThrowingStream<Data, Swift.Error> { continuation in
            dataStreamContinuation = continuation
            for message in dataToReceive {
                continuation.yield(message)
                if let string = String(data: message, encoding: .utf8) {
                    receivedMessages.append(string)
                }
            }
            dataToReceive.removeAll()

            if let receiveErrorOnReceive {
                continuation.finish(throwing: receiveErrorOnReceive)
                self.receiveErrorOnReceive = nil
                dataStreamContinuation = nil
            } else if shouldFinishOnReceive {
                continuation.finish()
                shouldFinishOnReceive = false
                dataStreamContinuation = nil
            }
        }
    }

    func setFailConnect(_ shouldFail: Bool) {
        shouldFailConnect = shouldFail
    }

    func setFailSend(_ shouldFail: Bool) {
        shouldFailSend = shouldFail
    }

    func setFailOnSendAttempt(_ attempt: Int?) {
        failOnSendAttempt = attempt
    }

    func finishReceiving() {
        if let continuation = dataStreamContinuation {
            continuation.finish()
            dataStreamContinuation = nil
        } else {
            shouldFinishOnReceive = true
        }
    }

    func failReceiving(with error: MCPError = MCPError.transportError(POSIXError(.EIO))) {
        if let continuation = dataStreamContinuation {
            continuation.finish(throwing: error)
            dataStreamContinuation = nil
        } else {
            receiveErrorOnReceive = error
        }
    }

    func queue(data: Data) {
        if let continuation = dataStreamContinuation {
            continuation.yield(data)
        } else {
            dataToReceive.append(data)
        }
    }

    func queue<M: Method>(request: Request<M>) throws {
        queue(data: try encoder.encode(request))
    }

    func queue<M: Method>(response: Response<M>) throws {
        queue(data: try encoder.encode(response))
    }

    func queue<N: Notification>(notification: Message<N>) throws {
        queue(data: try encoder.encode(notification))
    }

    func queue(batch requests: [AnyRequest]) throws {
        queue(data: try encoder.encode(requests))
    }

    func queue(batch responses: [AnyResponse]) throws {
        queue(data: try encoder.encode(responses))
    }

    func decodeLastSentMessage<T: Decodable>() -> T? {
        guard let lastMessage = sentData.last else { return nil }
        do {
            return try decoder.decode(T.self, from: lastMessage)
        } catch {
            return nil
        }
    }

    func clearMessages() {
        sentData.removeAll()
        sendAttempts = 0
        dataToReceive.removeAll()
    }
}
