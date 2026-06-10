import { useMemo, useState } from 'react';



const API_BASE = import.meta.env.VITE_API_BASE || 'http://localhost';



const VEHICLES = [

  { id: 'V001', plate: 'KA-01-1001', year: 2022, name: 'Swift', type: 'Hatchback', seats: 5 },

  { id: 'V002', plate: 'KA-01-1002', year: 2022, name: 'City', type: 'Sedan', seats: 5 },

  { id: 'V003', plate: 'KA-01-1003', year: 2022, name: 'Creta', type: 'SUV', seats: 5 },

  { id: 'V004', plate: 'KA-01-1004', year: 2023, name: 'Innova', type: 'MUV', seats: 7 },

  { id: 'V005', plate: 'KA-01-1005', year: 2023, name: 'Thar', type: 'SUV', seats: 4 },

  { id: 'V006', plate: 'KA-01-1006', year: 2024, name: 'i20', type: 'Hatchback', seats: 5 },

  { id: 'V007', plate: 'KA-01-1007', year: 2024, name: 'Slavia', type: 'Sedan', seats: 5 },

  { id: 'V008', plate: 'KA-01-1008', year: 2024, name: 'XUV700', type: 'SUV', seats: 7 },

  { id: 'V009', plate: 'KA-01-1009', year: 2025, name: 'Harrier', type: 'SUV', seats: 5 },

  { id: 'V010', plate: 'KA-01-1010', year: 2025, name: 'Virtus', type: 'Sedan', seats: 5 },

  { id: 'V011', plate: 'KA-01-1011', year: 2025, name: 'Seltos', type: 'SUV', seats: 5 },

  { id: 'V012', plate: 'KA-01-1012', year: 2025, name: 'Baleno', type: 'Hatchback', seats: 5 },

];



const COLOR_BY_KIND = {

  book: 'book',

  payment: 'payment',

  success: 'success',

  error: 'error',

  info: 'info',

  ticket: 'ticket',

};



function App() {

  const [selected, setSelected] = useState([]);

  const [userId, setUserId] = useState('alice');

  const [fromDate, setFromDate] = useState(1);

  const [toDate, setToDate] = useState(3);

  const [logs, setLogs] = useState([]);

  const [isSubmitting, setIsSubmitting] = useState(false);



  const selectedLabel = useMemo(() => {

    if (!selected.length) {

      return 'Click cards to select vehicles';

    }

    return `Selected: ${selected.join(', ')}`;

  }, [selected]);



  const addLog = (msg, kind = 'info') => {

    const entry = {

      id: `${Date.now()}-${Math.random().toString(16).slice(2)}`,

      time: new Date().toLocaleTimeString(),

      msg,

      kind: COLOR_BY_KIND[kind] || 'info',

    };

    setLogs((prev) => [entry, ...prev]);

  };



  const toggleVehicle = (id) => {

    setSelected((prev) => {

      if (prev.includes(id)) {

        return prev.filter((v) => v !== id);

      }

      return [...prev, id];

    });

  };



  const pollTicket = (bookingId) => {

    const short = bookingId.slice(0, 8);

    addLog(`Polling for ticket [${short}...]`, 'payment');



    let attempts = 0;

    const timer = setInterval(async () => {

      attempts += 1;

      if (attempts > 20) {

        clearInterval(timer);

        addLog('No ticket after 16s. Check Booking, Payment, Ticket services and EventStreamer.', 'error');

        return;

      }



      try {

        const response = await fetch(`${API_BASE}/ticket/${bookingId}`);

        const data = await response.json();



        if (data.status === 'confirmed') {

          clearInterval(timer);

          addLog(

            `TICKET CONFIRMED booking=${short} vehicles=${data.vehicle_ids.join(',')} days=${data.from_date}-${data.to_date} user=${data.user_id}`,

            'ticket'

          );

        } else if (data.status === 'failed') {

          clearInterval(timer);

          addLog(`BOOKING FAILED ${data.reason || 'unknown reason'}`, 'error');

        }

      } catch (error) {

        clearInterval(timer);

        addLog(`Ticket poll failed: ${error.message}`, 'error');

      }

    }, 800);

  };



  const onSubmit = async (event) => {

    event.preventDefault();



    if (!selected.length) {

      addLog('Select at least one vehicle first.', 'error');

      return;

    }

    if (Number(fromDate) > Number(toDate)) {

      addLog('from_date must be <= to_date', 'error');

      return;

    }



    const payload = {

      vehicle_ids: selected,

      user_id: userId.trim() || 'guest',

      from_date: Number(fromDate),

      to_date: Number(toDate),

    };



    setIsSubmitting(true);



    try {

      addLog(

        `-> [1/4] BookEvent vehicles=${payload.vehicle_ids.join(',')} days=${payload.from_date}-${payload.to_date} user=${payload.user_id}`,

        'book'

      );



      const response = await fetch(`${API_BASE}/book`, {

        method: 'POST',

        headers: { 'Content-Type': 'application/json' },

        body: JSON.stringify(payload),

      });



      if (!response.ok) {

        const err = await response.json().catch(() => ({ detail: 'booking request failed' }));

        addLog(`Request failed: ${err.detail || 'unknown error'}`, 'error');

        return;

      }



      const data = await response.json();

      const shortBooking = data.booking_id.slice(0, 8);

      addLog(`-> [2/4] PaymentRequestEvent booking=${shortBooking}...`, 'payment');

      addLog('-> [3/4] PaymentSuccessEvent pending processor...', 'payment');

      pollTicket(data.booking_id);

    } catch (error) {

      addLog(`Network error: ${error.message}`, 'error');

    } finally {

      setIsSubmitting(false);

    }

  };



  return (

    <div className="app-shell">

      <div className="bg-blur one" />

      <div className="bg-blur two" />



      <main className="container">

        <header className="hero">

          <div>

            <p className="eyebrow">Event-driven microservices demo</p>

            <h1>Car Rental Frontend</h1>

            <p className="subtext">

              BookEvent {'->'} PaymentRequestEvent {'->'} PaymentSuccessEvent {'->'} GenerateTicketEvent

            </p>

          </div>

          <div className="api-pill">API: {API_BASE}</div>

        </header>



        <section className="grid">

          <article className="panel">

            <div className="panel-header">

              <h2>Choose Vehicles</h2>

              <span>{selected.length} selected</span>

            </div>



            <div className="vehicle-grid">

              {VEHICLES.map((car) => {

                const active = selected.includes(car.id);

                return (

                  <button

                    key={car.id}

                    type="button"

                    onClick={() => toggleVehicle(car.id)}

                    className={`vehicle-card ${active ? 'active' : ''}`}

                  >

                    <div className="vehicle-top">

                      <strong>{car.id}</strong>

                      <span>{car.type}</span>

                    </div>

                    <h3>{car.name}</h3>

                    <p>{car.plate}</p>

                    <small>

                      {car.year} • {car.seats} seats

                    </small>

                  </button>

                );

              })}

            </div>



            <p className="selection-label">{selectedLabel}</p>

          </article>



          <article className="panel">

            <div className="panel-header">

              <h2>New Booking</h2>

              <span>Fast flow</span>

            </div>



            <form onSubmit={onSubmit} className="booking-form">

              <label>

                User ID

                <input value={userId} onChange={(e) => setUserId(e.target.value)} placeholder="alice" />

              </label>



              <div className="two-col">

                <label>

                  From (day)

                  <input

                    type="number"

                    min="1"

                    value={fromDate}

                    onChange={(e) => setFromDate(e.target.value)}

                  />

                </label>

                <label>

                  To (day)

                  <input

                    type="number"

                    min="1"

                    value={toDate}

                    onChange={(e) => setToDate(e.target.value)}

                  />

                </label>

              </div>



              <button type="submit" disabled={isSubmitting}>

                {isSubmitting ? 'Booking...' : 'Book Now'}

              </button>

            </form>

          </article>

        </section>



        <section className="panel log-panel">

          <div className="panel-header">

            <h2>Event Log</h2>

            <button className="ghost" type="button" onClick={() => setLogs([])}>

              Clear

            </button>

          </div>



          <div className="log-list">

            {!logs.length && <p className="empty-state">Events will appear here once you make a booking.</p>}

            {logs.map((entry) => (

              <div className={`log-row ${entry.kind}`} key={entry.id}>

                [{entry.time}] {entry.msg}

              </div>

            ))}

          </div>

        </section>

      </main>

    </div>

  );

}



export default App;