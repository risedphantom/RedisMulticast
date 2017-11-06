'use strict'

class Matrix extends Array {
  /**
   * @param {int} rows
   * @param {int} columns
   * @param {boolean} [rand]
   */
  constructor (rows, columns, rand) {
    super()
    for (let i = 0; i < columns; i++) {
      this[i] = []
      for (let j = 0; j < rows; j++) {
        this[i][j] = rand === true ? (Math.random() * 100 | 0) : undefined
      }
    }
  }
}

class SquareMatrix extends Matrix {
  /**
   * @param {int} size
   * @param {boolean} [rand]
   */
  constructor (size, rand) {
    super(size, size, rand)
  }

  untwist () {
    let sign = 1
    let len = this.length
    let res = []
    let curRow = (len - 1) / 2
    let curCol = curRow

    res.push(this[curRow][curCol])
    for (let i = 1; i < len + 1; i++) {
      // Output row
      for (let j = 1; j <= Math.min(i, len - 1); j++) {
        res.push(this[curRow][curCol - sign])
        curCol -= sign
      }

      if (i === len) continue

      // Output column
      for (let j = 1; j <= i; j++) {
        res.push(this[curRow + sign][curCol])
        curRow += sign
      }

      sign *= -1
    }
    return res
  }
}

const n = 3
let mat = new SquareMatrix(2 * n - 1, true)

console.log('-= Source matrix =-')
console.log(mat)

console.log('\n-= Untwisted matrix =-')
console.log(mat.untwist().toString())
