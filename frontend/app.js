const socket = new WebSocket("ws://localhost:8080/ws");

const status = document.getElementById("status");
const messages = document.getElementById("messages");
const input = document.getElementById("messageInput");
const sendButton = document.getElementById("sendButton");

socket.onopen = () => {
    status.textContent = "Connected";
    console.log("WebSocket connected");
};

socket.onmessage = (event) => {
    const data = JSON.parse(event.data);

    const div = document.createElement("div");

    div.textContent =
        `${data.userId}: ${data.message}`;

    messages.appendChild(div);
};

socket.onerror = (error) => {
    console.error("WebSocket error:", error);
};

socket.onclose = () => {
    status.textContent = "Disconnected";
};

sendButton.addEventListener("click", sendMessage);

function sendMessage() {
    if (socket.readyState !== WebSocket.OPEN) {
        console.error("WebSocket is not connected");
        return;
    }

    const message = {
        type: "chat",
        room: "general",
        userId: "user-123",
        message: input.value,
        timestamp: new Date().toISOString()
    };

    socket.send(JSON.stringify(message));

    input.value = "";
}