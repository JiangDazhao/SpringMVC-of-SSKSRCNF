package com.jiang.service;

import org.ujmp.core.Matrix;
import org.ujmp.jmatio.ImportMatrixMAT;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;


public class Data {
       short [][] rawimg2D;
       double [][] img2D;
     // static double [][][] img3D;
       int [][] img_gt;
       int [] trainidx2D;
       int [] testidx2D;
       int [] totalidx2D;
       short [] trainlab;
       short [] testlab;
       short [] totallab;

       int rows;
       int cols;
       int bands;
    public Data(String sdataset, String strainidx2D, String stestidx2D, String groundtruth, String stotalidx2D) throws IOException {
        //load img data
        String datapathname="./resources/"+sdataset;
        String trainidxpathname="./resources/"+strainidx2D;
        String testidxpathname="./resources/"+stestidx2D;
        String totalidxpathname="./resources/"+stotalidx2D;
        ImportMatrixMAT testimg = new ImportMatrixMAT();
        File imgfile  = new File(datapathname);
        Matrix Matriximg = testimg.fromFile(imgfile);
        long[] img_dimentions=Matriximg.getSize();
        this.rows = (int)img_dimentions[0];
        this.cols = (int)img_dimentions[1];
        this.bands= (int)img_dimentions[2];

        // load img_gt
        String gtpathname="./resources/"+groundtruth;
        ImportMatrixMAT groundimg = new ImportMatrixMAT();
        File groundimgfile  = new File(gtpathname);
        Matrix Matrixgroundimg = groundimg.fromFile(groundimgfile);
        this.img_gt=new int[rows][cols];
        for(int i=0;i<rows;i++)
            for(int j=0;j<cols;j++){
                this.img_gt[i][j]=Matrixgroundimg.getAsInt(i,j);
                 //System.out.println(Matrixgroundimg.getAsInt(i,j));
            }


        //trainidx2D
        ImportMatrixMAT import_train = new ImportMatrixMAT();
        File trainfile = new File(trainidxpathname);
        Matrix matrix_train = import_train.fromFile(trainfile);
        long[] train_dim = matrix_train.getSize();
        int trainlen = (int) train_dim[1];
        this.trainidx2D = new int[trainlen];
        for(int i=0;i<trainlen;i++) {
            this.trainidx2D[i]= matrix_train.getAsInt(0,i)-1;
        }

        //testidx2D
        ImportMatrixMAT import_test = new ImportMatrixMAT();
        File testfile = new File(testidxpathname);
        Matrix matrix_test = import_test.fromFile(testfile);
        long[] test_dim = matrix_test.getSize();
        int testlen = (int) test_dim[1];
        this.testidx2D = new int[testlen];
        for(int i=0;i<testlen;i++)
            this.testidx2D[i]= matrix_test.getAsInt(0,i)-1;

        //totalidx2D
        ImportMatrixMAT import_total= new ImportMatrixMAT();
        File totalfile = new File(totalidxpathname);
        Matrix matrix_total = import_total.fromFile(totalfile);
        long[] total_dim = matrix_total.getSize();
        int totallen = (int) total_dim[1];
        this.totalidx2D = new int[totallen];
        for(int i=0;i<totallen;i++)
            this.totalidx2D[i]= matrix_total.getAsInt(0,i)-1;

        //trainlab
        this.trainlab=new short[trainidx2D.length];
        for(int i=0;i<trainlab.length;i++){
            int gdrow= trainidx2D[i]%rows;
            int gdcol=trainidx2D[i]/cols;
            trainlab[i]=(short)img_gt[gdrow][gdcol];
        }

        //testlab
        this.testlab=new short[testidx2D.length];
        for(int i=0;i<testlab.length;i++){
            int gdrow= testidx2D[i]%rows;
            int gdcol=testidx2D[i]/cols;
            testlab[i]=(short)img_gt[gdrow][gdcol];
        }

        //totallab
        this.totallab=new short[totalidx2D.length];
        for(int i=0;i<totallab.length;i++){
            int gdrow= totalidx2D[i]%rows;
            int gdcol=totalidx2D[i]/cols;
            totallab[i]=(short)img_gt[gdrow][gdcol];
        }


        //reshape3d_2d
        this.rawimg2D = new short[bands][rows*cols];
        for(int i=0;i<bands;i++)
            for(int j=0;j<cols;j++)
                for(int k=0;k<rows;k++)
                {
                    rawimg2D[i][j*rows+k]=Matriximg.getAsShort(k,j,i);
                }

        //line_dat
        double[] sortX = new double[rows*cols*bands];
        for(int i=0;i<rawimg2D.length;i++)
            for(int j=0;j<rawimg2D[0].length;j++)
            {
                sortX[i*rawimg2D[0].length+j]=rawimg2D[i][j];
            }
        Arrays.sort(sortX);
        int sortXL=sortX.length;
        double rdown = 0.001;
        double rup = 0.999;
        double lmin= sortX[Math.max((int)Math.ceil(sortXL*rdown),1)];
        //System.out.println(lmin);
        double lmax= sortX[Math.min((int)Math.floor(sortXL*rup),sortXL)];
        //System.out.println(lmax);

        this.img2D = new double[bands][rows*cols];
        for(int i=0;i<rawimg2D.length;i++)
            for(int j=0;j<rawimg2D[0].length;j++)
            {
                if(rawimg2D[i][j]<lmin) img2D[i][j]=lmin;
                else if(rawimg2D[i][j]>lmax) img2D[i][j]=lmax;
                img2D[i][j]=(rawimg2D[i][j]-lmin)/lmax;
            }


//        this.img3D=new double[rows][cols][bands];
//        for (int i=0;i<bands;i++)
//            for(int j=0;j<rows*cols;j++)
//            {
//                int rowth= j%rows;
//                int colth= j/rows;
//                img3D[rowth][colth][i]=img2D[i][j];
//            }
    }
    public short[][] getRawimg2D() {
        return rawimg2D;
    }

    public double[][] getImg2D() {
        return img2D;
    }

    public int[][] getImg_gt() {
        return img_gt;
    }

    public int[] getTrainidx2D() {
        return trainidx2D;
    }

    public int[] getTestidx2D() {
        return testidx2D;
    }

    public short[] getTrainlab() {
        return trainlab;
    }

    public short[] getTestlab() {
        return testlab;
    }

    public int[] getTotalidx2D() {
        return totalidx2D;
    }

    public short[] getTotallab() {
        return totallab;
    }

    public int getRows() {
        return rows;
    }

    public int getCols() {
        return cols;
    }

    public int getBands() {
        return bands;
    }

}
