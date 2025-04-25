// const api_url = "http://127.0.0.1:8000"
const api_url = "https://etl-tutorial.onrender.com"
// const api_url = "https://test-etl-1-0-0.onrender.com"

async function login(event) {
    event.preventDefault();
    const formData = new FormData(document.getElementById('login-form'));
    const response = await fetch(api_url+'/token', {
        method: 'POST',
        body: formData
    });

    if (response.ok) {
        const result = await response.json();
        localStorage.setItem('jwt', result.access_token);
        window.location.href = 'dashboard.html';
    } else {
        alert('Login failed!');
    }
}

function logout() {
    // Remove the token from local storage
    localStorage.removeItem('jwt');
    window.location.href = 'index.html';
}

async function fetchData(url, token) {
    const response = await fetch(url, {
        method: 'GET',
        headers: {
            Authorization: `Bearer ${token}`
        }
    });

    if (response.ok) {
        const data = await response.json();
        return data;
    } else {
        return 'Access Denied';
    }
}

async function displayData(token) {
    const monthOrder = {
        January: 1,
        February: 2,
        March: 3,
        April: 4,
        May: 5,
        June: 6,
        July: 7,
        August: 8,
        September: 9,
        October: 10,
        November: 11,
        December: 12
    };

    try {
        const jwtPayload = JSON.parse(atob(token.split('.')[1]));
        const roles = jwtPayload.roles;
        const user = jwtPayload.name
        let dataUrl = '/data/common'; // Default to common data
        if (roles.includes('manager')) {
            dataUrl = '/data/manager'; // Manager-specific data
        } else if (roles.includes('employee')) {
            dataUrl = '/data/employee'; // Employee-specific data
        }
        
        const datain = await fetchData(api_url+`${dataUrl}`, token);
        document.getElementById('message-display').innerText = "Logged in as " + user

        try {
            if (dataUrl=="/data/manager") {
                const data = JSON.parse(datain)[0]
            new Chart(
                document.getElementById('separate-pay'),
                {
                  type: 'bar',
                  data: {
                    labels: data.map(row => row.Name),
                    datasets: [
                        {
                        label: 'Hourly_Pay',
                        data: data.map(row => row['Hourly_Pay']),
                        backgroundColor: '#f2cbae',
                        },
                        {
                        label: 'Travel_Pay',
                        data: data.map(row => row['Travel_Pay']),
                        backgroundColor: '#ebb4d3',
                        },
                        {
                        label: 'Weather_Pay',
                        data: data.map(row => row['Weather_Pay']),
                        backgroundColor: '#2b2a65',
                        },
                    ]
                  },
                  options : {
                    plugins: {
                    title: {
                        display: true,
                        text: 'Salary Paid by Category'
                    }
                }
                  }
                }
              );
              const total = JSON.parse(datain)[1]
              new Chart(
                document.getElementById('total-pay'),
                {
                  type: 'pie',
                  options : {
                    plugins: {
                    title: {
                        display: true,
                        text: 'Total Salary Received'
                    }
                }
                  },
                  data: {
                    labels: total.map(row => row.Name),
                    datasets: [
                        {data: total.map(row => row['Total_Pay']),}
                    ]
                  },
                  
                }
              );
        } else if (dataUrl=="/data/employee") {
            const data = JSON.parse(datain)[0]
            
            const unorderedMonths = data.map(row => row.date);
            // Create an array of indices from the unorderedMonths
            const indices = unorderedMonths.map((month, index) => ({ index, month }));

            // Sort the indices array based on the month order
            indices.sort((a, b) => monthOrder[a.month] - monthOrder[b.month]);

            new Chart(
                document.getElementById('separate-pay'),
                {
                  type: 'bar',
                  options : {
                    plugins: {
                    title: {
                        display: true,
                        text: 'Monthly Pay by Category'
                    }
                    }
                  },
                  data: {
                    labels: indices.map(item => unorderedMonths[item.index]),
                    datasets: [
                        {
                        label: 'Hourly_Pay',
                        data: indices.map(item => data.map(row => row['Hourly_Pay'])[item.index]),
                        backgroundColor: '#f2cbae',
                        },
                        {
                        label: 'Travel_Pay',
                        data: indices.map(item => data.map(row => row['Travel_Pay'])[item.index]),
                        backgroundColor: '#ebb4d3',
                        },
                        {
                        label: 'Weather_Pay',
                        data: indices.map(item => data.map(row => row['Weather_Pay'])[item.index]),
                        backgroundColor: '#2b2a65',
                        },
                    ]
                  },
                }
              );
              ;
              const total = JSON.parse(datain)[1]
              new Chart(
                document.getElementById('total-pay'),
                {
                  type: 'pie',
                  data: {
                    labels: total.map(row => row.date),
                    datasets: [
                        {data: total.map(row => row['Total_Hours']),}
                    ]
                  },
                  options : {
                    plugins: {
                    title: {
                        display: true,
                        text: 'Hours Worked per Month'
                    }
                }
                  },
                }
              );
        }
        } catch {
            document.getElementById('message-display').innerText = "Logged in as " + user
            document.getElementById('data-display').innerText = datain;
        }

    } catch (error) {
        console.error('Error displaying data:', error);
        document.getElementById('data-display').innerText = 'You need to be logged in / authorized to view the data.';
    }
}

function checkLogin() {
    const token = localStorage.getItem('jwt');
    if (token) {
        displayData(token);
    }
}

window.onload = checkLogin;