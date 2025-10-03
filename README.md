# End-to-End-Machine-Learning-Rain-Prediction

## Workflow step

### Early Setup
1. Create an repo on your github.
2. Clone the repo into your work directory.
3. Setup your conda environment, you can see steps below.
4. Open a VS Code, then run template.py on your terminal.

### Data Ingestion
1. Download rain prediction kaggle dataset via link mentioned above
2. 

## How to create and run conda environment?

Set your conda name and python version. My conda name is rain and python version is 3.11.3.

```bash
conda create -n rain python=3.11.3 -y
```

Initializing your conda before activate. Type bash if you're using git bash.

```bash
conda init bash 
```

Then, activate your conda environment.

```bash
conda activate rain
```

finally, install  the requirements.txt

```bash
pip install -3 requirements.txt
```

## How to export the environment variable?

```bash
export MONGODB_URL="mongodb+srv://<username>:<password>...."

export AWS_ACCESS_KEY_ID=<AWS_ACCESS_KEY_ID>

export AWS_SECRET_ACCESS_KEY=<AWS_SECRET_ACCESS_KEY>
```
