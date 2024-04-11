set windows-shell:=["powershell.exe","-NoLogo","-Command"]
zlm:
    cd D:\klwork\wvp_pro_compose;just up zlm;
wvp:
    cd D:\klwork\wvp-GB28181-pro\web_src;start just dev
dev:
    npm run dev
merge:
    cmd /c start xiu -r 9977
    cmd /c start ffmpeg -i rtsp://admin:cl8353393@654321@192.168.30.120:554/LiveMedia/ch1/Media1 -i rtsp://admin:cl8353393@654321@192.168.30.111:554/h264/ch33/main/av_stream -filter_complex "[0:v][1:v]hstack=inputs=2[v]" -map "[v]" -f flv rtmp://127.0.0.1:9977/merged_stream
    cmd /c call "C:\Program Files (x86)\VideoLAN\VLC\vlc.exe" rtmp://127.0.0.1:9977/merged_stream
