# Aplikasi Chatting RabbitMQ (Publisher-cmd & Consumer-cmd)

Implementasi modul *Messaging Service menggunakan RabbitMQ* bagian 1.5 dan 1.6 dengan **C# (.NET 8)** dan **RabbitMQ.Client 6.8.1**.

## Prasyarat
- RabbitMQ berjalan di `localhost:5672` (user `guest` / `guest`), lewat Docker atau terpasang langsung di Windows.
- .NET 8 SDK (atau Visual Studio 2022).

Menjalankan RabbitMQ dengan Docker:

```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

## Menjalankan
Buka terminal terpisah untuk setiap aplikasi, di folder project ini:

```bash
dotnet run --project Consumer-cmd
dotnet run --project Publisher-cmd
```

## Perbedaan dengan kode modul
| Bagian | Modul | Di project ini | Alasan |
|---|---|---|---|
| Consumer project | Universal Windows Blank App | Console App | Kode modul memakai `Console`, jadi aplikasinya console |
| `Encoding.UTF8.GetString(body)` | `body` langsung | `ea.Body.ToArray()` | Di RabbitMQ.Client 6.x `Body` bertipe `ReadOnlyMemory<byte>` |
| Channel consumer | `using (channel = ...)` lalu `Disconnect()` | Channel dibuka sekali, ditutup di `Disconnect()` | Menghindari channel ditutup dua kali |
| `BasicPublish` | tanpa `mandatory` | `mandatory: true` + event `BasicReturn` | Publisher menampilkan peringatan jika tidak ada queue dengan nama tujuan |
| `connectionFactory.Port` | dikomentari | diaktifkan | Port tetap 5672 secara default |

## Eksperimen
- **1P-1C:** consumer dan publisher memakai queue channel dengan nama yang sama, lalu publisher mengirim ke tujuan yang sama dan ke tujuan lain.
- **1P-2C:** dua consumer dengan nama queue berbeda, lalu publisher memilih tujuan.

Pesan dikirim lewat *default exchange* (`""`), yang meneruskan pesan ke queue yang namanya sama persis dengan routing key (`tujuan`). Jika tidak ada queue dengan nama itu, pesan dibuang oleh broker.
