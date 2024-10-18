document.addEventListener('DOMContentLoaded', function() {
    fetch('/get_history')
        .then(response => response.json())
        .then(data => {
            data.history.forEach(message => {
                appendMessage(message.sender, message.text);
            });
        });
});

document.getElementById('sendButton').addEventListener('click', sendMessage);
document.getElementById('userInput').addEventListener('keypress', function(e) {
    if (e.key === 'Enter') {
        sendMessage();
    }
});

function sendMessage() {
    const userInputField = document.getElementById('userInput');
    const userInput = userInputField.value.trim();
    if (userInput === '') return;

    appendMessage('user', userInput);
    userInputField.value = '';
    userInputField.focus();

    // Send user input to the backend
    fetch('/chat', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({ message: userInput })
    })
    .then(response => response.json())
    .then(data => {
        appendMessage('agent', data.response);
    })
    .catch(error => {
        console.error('Error:', error);
        appendMessage('agent', 'Sorry, an error occurred.');
    });
}

function appendMessage(sender, message) {
    const chatbox = document.getElementById('chatbox');
    const messageWrapper = document.createElement('div');
    messageWrapper.classList.add('message', sender);

    const messageContent = document.createElement('div');
    messageContent.classList.add('message-content');
    messageContent.innerText = message;

    messageWrapper.appendChild(messageContent);
    chatbox.appendChild(messageWrapper);
    chatbox.scrollTop = chatbox.scrollHeight;
}
