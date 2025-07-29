# frozen_string_literal: true

# The postgres broadcast adapter is now loaded from the anycable-rb fork
# require_relative '../../../lib/anycable/broadcast_adapters/postgres'

feature "PostgreSQL Broadcast" do
  channel do
    def subscribed
      stream_from "a"
    end
  end

  scenario %(
    Receives broadcast from PostgreSQL
  ) do
    subscribe_request = {command: "subscribe", identifier: {channel: channel}.to_json}

    client.send(subscribe_request)

    ack = {
      "identifier" => {channel: channel}.to_json, "type" => "confirm_subscription"
    }

    assert_message ack, client.receive

    # Broadcast a message through our postgres adapter
    ActionCable.server.broadcast("a", {"text" => "Hello from Postgres!"})

    msg = {"identifier" => {channel: channel}.to_json, "message" => {"text" => "Hello from Postgres!"}}

    assert_message msg, client.receive
  end

  scenario %(
    Receives large payload broadcast from PostgreSQL
  ) do
    subscribe_request = {command: "subscribe", identifier: {channel: channel}.to_json}

    client.send(subscribe_request)

    ack = {
      "identifier" => {channel: channel}.to_json, "type" => "confirm_subscription"
    }

    assert_message ack, client.receive

    # Create a large payload (larger than 8000 bytes default threshold)
    large_text = "A" * 9000
    
    # Broadcast a large message through our postgres adapter
    ActionCable.server.broadcast("a", {"text" => large_text})

    msg = {"identifier" => {channel: channel}.to_json, "message" => {"text" => large_text}}

    assert_message msg, client.receive
  end
end
