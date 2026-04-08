package com.kmarinov.rtdbms.pi.devices.bmp280;

public interface BMP280Interface {


    /**
     * Perform any/all actions to reset the sensor
     */
    void resetSensor();


    /**
     * Perform any/all actions to initialize  the sensor
     */
    void initSensor();

    /*** @return temperature in centigrade
     */
    double temperatureC();

    /**
     * @return temperature in fahrenheit
     */
    double temperatureF();


    /**
     * @return presure in Pa units
     */
    double pressurePa();

    /**
     * @return pressure in Inches Mercury
     */
    double pressureIn();

    /**
     * @return pressure in millibar
     */
    double pressureMb();

    /**
     * @param register
     * @return 8bit value read from register
     */
    int readRegister(int register);

    /**
     *
     * @param register   Multi byte register address
     * @param buffer     Buffer to return read data
     * @return count     number bytes read or fail -1
     */
    //   int readRegister(byte[] register, byte[] buffer);

    /**
     * @param register register address
     * @param buffer   Buffer to return read data
     * @return count     number bytes read or fail -1
     */
    int readRegister(int register, byte[] buffer);

    /**
     *
     * @param register  multi byte register
     * @param data      byte array data to write
     * @param dataLength  lentgh of data to write
     * @return bytes written, else -1
     */
    //   int writeRegister(byte[] register, byte[] data, int dataLength);


    /**
     * @param register byte register
     * @param data     byte data to write
     * @return bytes written, else -1
     */
    int writeRegister(int register, int data);

    /**
     * @param writeData       bytes to write
     * @param readData        bytes to read
     * @param afterWriteDelay MS time to delay after write
     */
    public void writeDelayRead(byte[] writeData, short afterWriteDelay, byte[] readData);


}


