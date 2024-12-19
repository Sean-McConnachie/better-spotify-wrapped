# Spotify History

## A more detailed version of Spotify wrapped for your whole listening history.

### How to use

#### Setup [Ollama](https://github.com/ollama/ollama) 

1. Install [Ollama](https://github.com/ollama/ollama) (GPU highly reccommened)
2. Run `ollama pull llama3.2`
3. Run `ollama serve`

**Note:** For some reason I needed to run this command on Linux to get my GPU to work.

```bash
sudo systemctl stop ollama
sudo nvidia-modprobe -u
sudo rmmod nvidia_uvm
sudo modprobe nvidia_uvm
sudo systemctl start ollama
```

#### How to add/change genres

1. Update `config.json` with your chosen genres
2. Run `python classify_genres.py` OR just run the jupyter notebook.
2. Make a coffee and sit back.

#### Running your data