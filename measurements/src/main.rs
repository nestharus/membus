use std::{env,fs::{File,OpenOptions},io::{self,Read,Write},net::{TcpListener,TcpStream},sync::{Arc,Barrier},thread,time::Instant};
use std::os::windows::{fs::OpenOptionsExt,io::FromRawHandle};
type Handle=*mut std::ffi::c_void;
#[link(name="kernel32")]
extern "system" {
 fn CreateNamedPipeW(name:*const u16, mode:u32, flags:u32, instances:u32, out:u32, input:u32, timeout:u32, security:*mut std::ffi::c_void)->Handle;
 fn ConnectNamedPipe(h:Handle, overlap:*mut std::ffi::c_void)->i32;
 fn GetLastError()->u32;
}
fn pipe(name:&str)->io::Result<File>{
 let wide:Vec<u16>=name.encode_utf16().chain(Some(0)).collect();
 let h=unsafe{CreateNamedPipeW(wide.as_ptr(),3,0,255,65536,65536,0,std::ptr::null_mut())};
 if h as isize == -1 { return Err(io::Error::last_os_error()); }
 Ok(unsafe{File::from_raw_handle(h)})
}
fn serve_stream<S:Read+Write>(mut stream:S,size:usize,n:usize)->io::Result<()> {
 let mut b=vec![0;size];
 for _ in 0..n { stream.read_exact(&mut b)?; stream.write_all(&b)?; }
 Ok(())
}
fn stats(kind:&str,size:usize,clients:usize, mut ns:Vec<u128>, seconds:f64, bytes:u64, flushes:u64,records:u64){
 ns.sort_unstable();
 let p=|q:f64|ns[((ns.len()-1) as f64*q).ceil() as usize] as f64/1000.;
 println!("{{\"kind\":\"{}\",\"payload\":{},\"clients\":{},\"samples\":{},\"p50_us\":{},\"p95_us\":{},\"p99_us\":{},\"seconds\":{},\"operations_per_sec\":{},\"bytes_written\":{},\"flushes\":{},\"records\":{},\"records_per_flush\":{}}}",kind,size,clients,ns.len(),p(0.5),p(0.95),p(0.99),seconds,records as f64/seconds,bytes,flushes,records,if flushes==0{0.}else{records as f64/flushes as f64});
}
fn flush(path:&str)->io::Result<()> {
 for mode in ["unflushed","flush","write_through","write_through_flush"] {
  for size in [4096,16384,65536] {
   let mut o=OpenOptions::new(); o.write(true).create(true).truncate(true);
   if mode.starts_with("write_through") {o.custom_flags(0x80000000);}
   let mut f=o.open(path)?;let b=vec![0x5a;size];let mut samples=Vec::new();let start=Instant::now();
   let explicit_flush=mode=="flush" || mode=="write_through_flush";
   for _ in 0..200 {let t=Instant::now();f.write_all(&b)?;if explicit_flush{f.sync_all()?;}samples.push(t.elapsed().as_nanos());}
   let sec=start.elapsed().as_secs_f64(); stats(mode,size,1,samples,sec,(size*200) as u64,if explicit_flush{200}else{0},200);
   // Drain the unflushed control outside its timed interval before the next case.
   if mode=="unflushed" {f.sync_all()?;println!("{{\"kind\":\"untimed_control_drain\",\"bytes_written\":0,\"flushes\":1}}");}
  }
 }
 for batch in [1usize,8,64,512] {
  let iterations=if batch>=64{64}else{200};
  let mut f=OpenOptions::new().write(true).create(true).truncate(true).open(path)?;
  let b=vec![0x5a;2253];let mut samples=Vec::new();let start=Instant::now();
  for _ in 0..iterations {let t=Instant::now();for _ in 0..batch{f.write_all(&b)?;}f.sync_all()?;samples.push(t.elapsed().as_nanos());}
  stats("group_commit",batch,1,samples,start.elapsed().as_secs_f64(),(2253*batch*iterations) as u64,iterations as u64,(batch*iterations) as u64);
 }
 Ok(())
}
fn server(transport:&str,address:&str,size:usize,clients:usize,n:usize)->io::Result<()> {
 let mut joins=Vec::new();
 if transport=="tcp" {
  let listener=TcpListener::bind(address)?;println!("READY {}",listener.local_addr()?);io::stdout().flush()?;
  for incoming in listener.incoming().take(clients) {let stream=incoming?;stream.set_nodelay(true)?;joins.push(thread::spawn(move||serve_stream(stream,size,n)));}
 }else{
  let mut files=Vec::new();for _ in 0..clients { files.push(pipe(address)?); }
  for f in files {joins.push(thread::spawn(move||{
   use std::os::windows::io::AsRawHandle;
   if unsafe{ConnectNamedPipe(f.as_raw_handle(),std::ptr::null_mut())}==0 && unsafe{GetLastError()}!=535 {return Err(io::Error::last_os_error());}
   serve_stream(f,size,n)
  }));}
  println!("READY {}",address);io::stdout().flush()?;
 }
 for j in joins {j.join().map_err(|_|io::Error::other("worker panic"))??;}Ok(())
}
fn roundtrips<S:Read+Write>(mut stream:S,size:usize,n:usize,barrier:Arc<Barrier>,id:u64)->io::Result<Vec<u128>>{
 let mut payload=vec![0x5a;size];let mut out=vec![0;size];let mut samples=Vec::new();
 for sequence in 0..n+16 {if sequence==16{barrier.wait();barrier.wait();}payload[..8].copy_from_slice(&((id<<32)|sequence as u64).to_le_bytes());let t=Instant::now();stream.write_all(&payload)?;stream.read_exact(&mut out)?;if sequence>=16{samples.push(t.elapsed().as_nanos());}if out!=payload{return Err(io::Error::other("echo differs"));}}
 Ok(samples)
}
fn client(transport:&str,address:&str,size:usize,clients:usize,n:usize)->io::Result<()> {
 let barrier=Arc::new(Barrier::new(clients+1));let mut joins=Vec::new();
 for id in 0..clients {let addr=address.to_owned();let b=barrier.clone();let tr=transport.to_owned();joins.push(thread::spawn(move||{
  if tr=="tcp" {let s=TcpStream::connect(addr)?;s.set_nodelay(true)?;roundtrips(s,size,n,b,id as u64)}else{roundtrips(OpenOptions::new().read(true).write(true).open(addr)?,size,n,b,id as u64)}
 }));}
 barrier.wait();let start=Instant::now();barrier.wait();let mut samples=Vec::new();for j in joins{samples.extend(j.join().map_err(|_|io::Error::other("worker panic"))??);}
 stats(&format!("rust_{}",transport),size,clients,samples,start.elapsed().as_secs_f64(),0,0,(clients*n) as u64);Ok(())
}
fn main()->io::Result<()> {
 let args:Vec<String>=env::args().collect();
 match args.get(1).map(String::as_str) {
  Some("flush") if args.len()==3=>flush(&args[2]),
  Some("server")|Some("client") if args.len()==7=>{
   let size=args[4].parse().map_err(|_|io::Error::other("invalid size"))?;let clients=args[5].parse().map_err(|_|io::Error::other("invalid clients"))?;let n=args[6].parse().map_err(|_|io::Error::other("invalid count"))?;
   if size<8 || size>1048576 || clients==0 || clients>254 || n==0 || !matches!(args[2].as_str(),"tcp"|"pipe") {return Err(io::Error::other("tcp/pipe, size 8..1048576, positive count, 1..254 clients required"));}
   if args[1]=="server" {server(&args[2],&args[3],size,clients,n)}else{client(&args[2],&args[3],size,clients,n)}
  },
  _=>Err(io::Error::other("usage: flush PATH | server/client tcp/pipe ADDRESS SIZE CLIENTS COUNT"))
 }
}
