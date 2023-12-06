package lambda;

/**
 *
 * @author Wes Lloyd
 */
public class Request {

    String name;
    private String bucketname;
    private String filename;
    private int row;
    private int col;

    public String getName() {
        return name;
    }

    public String getNameALLCAPS() {
        return name.toUpperCase();
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getBucketname() {
        return bucketname;
    }

    public void setBucketname(String name) {
        this.bucketname = name;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String name) {
        this.filename = name;
    }

    public int getCol() {
        return col;
    }

    public void setCol(int c) {
        this.col = c;
    }

    public int getRow() {
        return row;
    }

    public void setRow(int r) {
        this.row = r;
    }

    public Request(String name) {
        this.name = name;
    }

    public Request() {

    }
}
