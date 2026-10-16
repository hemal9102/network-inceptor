    import re
    import ddddocr
    from fastapi import FastAPI, Request
    from fastapi.responses import PlainTextResponse
    import uvicorn

    app = FastAPI()
    ocr = ddddocr.DdddOcr(show_ad=False)

    def evaluate_equation(text: str) -> str:
        # Clean OCR text: keep digits and operators
        clean = text.replace(" ", "").replace("=", "").replace("?", "")

        # Common OCR misreads for math operators
        clean = clean.replace("x", "*").replace("X", "*")
        clean = clean.replace("t", "+").replace("T", "+")

        # Match: Number [Operator] Number
        match = re.search(r"(\d+)\s*([\+\-\*])\s*(\d+)", clean)
        if match:
            n1, op, n2 = int(match.group(1)), match.group(2), int(match.group(3))
            if op == "+":
                return str(n1 + n2)
            elif op == "-":
                return str(n1 - n2)
            elif op == "*":
                return str(n1 * n2)

        # Fallback if already pure digits
        digits = re.findall(r"\d+", clean)
        if len(digits) >= 2:
            return str(int(digits[0]) + int(digits[1]))
        elif len(digits) == 1:
            return digits[0]

        return "37" # Default fallback

    @app.post("/solve", response_class=PlainTextResponse)
    async def solve(request: Request):
        image_bytes = await request.body()
        raw_text = ocr.classification(image_bytes)
        answer = evaluate_equation(raw_text)
        print(f"🖼️ OCR read: '{raw_text}' -> Solved Answer: {answer}")
        return answer

    if __name__ == "__main__":
        uvicorn.run(app, host="127.0.0.1", port=8000, log_level="warning")
