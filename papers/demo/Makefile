all: paper

paper: main.tex
	pdflatex main && bibtex main && pdflatex main && pdflatex main


clean:
	rm -f *.dvi *.aux *.log *.blg *.toc *.bbl *~ *.lof *.lot *.out *.fdb_latexmk *.fls *.synctex.gz
